using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Metadata;
using QueryKit.Sql;

namespace QueryKit.Extensions
{
    /// <summary>
    /// Provides synchronous CRUD extension methods for <see cref="IDbConnection"/> using Dapper.
    /// Parity with <see cref="ConnectionExtensionsAsync"/> (same SQL, quoting/encapsulation, allowlists, safeguards).
    /// </summary>
    public static class ConnectionExtensions
    {
        /// <summary>
        /// Gets the active SQL dialect configuration used by QueryKit.
        /// </summary>
        public static DialectConfig Config { get; private set; } = DialectConfig.Create(Dialect.SQLServer);

        /// <summary>
        /// Optional sink for generated SQL. Invoked by every CRUD method with the SQL it built.
        /// Defaults to writing to <see cref="Trace"/> when a debugger is attached, preserving
        /// QueryKit's historical behavior. Set to <c>null</c> to silence logging, or assign a
        /// custom delegate (e.g. <c>ILogger.LogDebug</c>) to capture SQL in production.
        /// </summary>
        public static Action<string>? Logger { get; set; } = msg =>
        {
            if (Debugger.IsAttached) Trace.WriteLine(msg);
        };

        internal static void Log(Func<string> messageFactory)
        {
            var logger = Logger;
            if (logger is null) return;
            logger(messageFactory());
        }

        // Single shared convention per dialect. Volatile so threads observe the swap promptly,
        // though UseDialect is expected to be called at process startup.
        private static volatile SqlConvention _convention = new(
            DialectConfig.Create(Dialect.SQLServer),
            new TableNameResolver(),
            new ColumnNameResolver());

        /// <summary>
        /// Sets the SQL dialect used for identifier quoting, identity retrieval, and paging SQL.
        /// </summary>
        public static void UseDialect(Dialect dialect)
        {
            var newConfig = DialectConfig.Create(dialect);
            var newConvention = new SqlConvention(newConfig, new TableNameResolver(), new ColumnNameResolver());
            Config = newConfig;
            _convention = newConvention;
            ColumnMapCache.Clear();
        }

        internal static SqlConvention NewConvention() => _convention;

        internal static SqlBuilder NewBuilder(SqlConvention conv) =>
            new SqlBuilder(conv);

        // cache: normalized name -> encapsulated column name for T + dialect
        internal static readonly ConcurrentDictionary<(Type, string), Dictionary<string, string>> ColumnMapCache = new();

        /// <summary>
        /// Builds an ascending order-by tuple for use with GetList.
        /// </summary>
        public static (Expression<Func<T, object>> Body, bool Descending) OrderByAscending<T>(Expression<Func<T, object>> e) => (e, false);

        /// <summary>
        /// Builds a descending order-by tuple for use with GetList.
        /// </summary>
        public static (Expression<Func<T, object>> Body, bool Descending) OrderByDescending<T>(Expression<Func<T, object>> e) => (e, true);

        /// <summary>
        /// Retrieves a single entity of type <typeparamref name="T"/> by its primary key.
        /// </summary>
        public static T? Get<T>(this IDbConnection connection, object id,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (id is null) throw new ArgumentNullException(nameof(id));

            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Get<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableNameEncapsulated(currentType);

            var sb = new StringBuilder();
            sb.Append("Select ");
            builder.BuildSelect(sb, SqlBuilder.GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0} where ", table);

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            var dyn = new DynamicParameters();
            if (idProps.Length == 1)
            {
                dyn.Add("@" + idProps[0].Name, id);
            }
            else
            {
                foreach (var p in idProps)
                {
                    var val = id.GetType().GetProperty(p.Name);
                    if (val == null)
                        throw new ArgumentException($"Missing key property '{p.Name}' on id object for {typeof(T).Name}.");
                    dyn.Add("@" + p.Name, val.GetValue(id, null));
                }
            }

            Log(() => $"Get<{currentType.Name}>: {sb} with Id: {id}");

            return connection.Query<T>(Cmd(sb.ToString(), dyn, transaction, commandTimeout)).FirstOrDefault();
        }

        /// <summary>
        /// Executes a stored procedure and maps the results to a list of <typeparamref name="T"/>.
        /// </summary>
        public static IList<T> ExecuteStoredProcedure<T>(this IDbConnection connection,
            string storedProcedureName, object? parameters = null,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var result = connection.Query<T>(StoredProc(storedProcedureName, parameters, transaction, commandTimeout));
            return result.ToList();
        }

        /// <summary>
        /// Queries entities using an anonymous object for equality-based filters with strongly-typed ORDER BY (ASC/DESC).
        /// </summary>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection,
            object? whereConditions,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            params (Expression<Func<T, object>> Body, bool Descending)[] orderBy)
        {
            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var currentType = typeof(T);
            var table = conv.GetTableNameEncapsulated(currentType);

            var sb = new StringBuilder();
            var whereProps = GetAllProperties(whereConditions)?.ToArray();

            sb.Append("Select ");
            builder.BuildSelect(sb, SqlBuilder.GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0}", table);

            if (whereProps != null && whereProps.Any())
            {
                sb.Append(" where ");
                builder.BuildWhere<T>(sb, whereProps, whereConditions);
            }

            if (orderBy.Length > 0)
            {
                var cols = new List<string>(orderBy.Length);

                foreach (var (body, desc) in orderBy)
                {
                    var me =
                        body.Body as MemberExpression ??
                        (body.Body as UnaryExpression)?.Operand as MemberExpression;

                    if (me?.Member is not PropertyInfo prop)
                        throw new ArgumentException("OrderBy must be a property access, e.g., x => x.LastName.");

                    var col = conv.GetColumnNameEncapsulated(prop);
                    if (string.IsNullOrEmpty(col))
                        throw new ArgumentException($"Property '{prop.Name}' is not mapped for {typeof(T).Name}.");

                    cols.Add(desc ? $"{col} DESC" : $"{col} ASC");
                }

                sb.Append(" order by ").Append(string.Join(", ", cols));
            }

            Log(() => $"GetList<{currentType.Name}>: {sb}");

            return connection.Query<T>(Cmd(sb.ToString(), whereConditions, transaction, commandTimeout));
        }

        /// <summary>
        /// Queries entities using a raw SQL WHERE fragment with optional parameters and raw ORDER BY (validated).
        /// </summary>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection,
            string conditions,
            object? parameters = null,
            string? orderBy = null,
            IDbTransaction? transaction = null,
            int? commandTimeout = null)
        {
            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var currentType = typeof(T);
            var table = conv.GetTableNameEncapsulated(currentType);

            var sb = new StringBuilder();
            sb.Append("Select ");
            builder.BuildSelect(sb, SqlBuilder.GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0}", table);

            if (!string.IsNullOrWhiteSpace(conditions))
            {
                if (!conditions.TrimStart().StartsWith("where", StringComparison.OrdinalIgnoreCase))
                    sb.Append(" where ");
                else
                    sb.Append(" ");
                sb.Append(conditions);
            }

            if (!string.IsNullOrWhiteSpace(orderBy))
            {
                Dictionary<string, string> allowed = BuildAllowedColumnMap<T>(conv);
                var validated = new List<string>();
                var parts = orderBy.Split(',');

                for (int i = 0; i < parts.Length; i++)
                {
                    var token = parts[i].Trim();
                    if (string.IsNullOrEmpty(token)) continue;

                    var bits = token.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    if (bits.Length == 0) continue;

                    var rawCol = bits[0];
                    var norm = NormalizeIdentifier(rawCol);
                    if (!allowed.TryGetValue(norm, out var encapsulated))
                        throw new ArgumentException("Invalid ORDER BY column '" + rawCol + "' for " + currentType.Name + ".");

                    var dir = (bits.Length > 1 ? bits[1] : "ASC")?.ToUpperInvariant();
                    if (dir != "ASC" && dir != "DESC")
                        throw new ArgumentException("Invalid ORDER BY direction '" + dir + "'. Use ASC or DESC.");

                    validated.Add(encapsulated + " " + dir);
                }

                if (validated.Count > 0)
                    sb.Append(" order by ").Append(string.Join(", ", validated));
            }

            Log(() => $"GetList<{currentType.Name}>: {sb}");

            return connection.Query<T>(Cmd(sb.ToString(), parameters, transaction, commandTimeout));
        }

        /// <summary>
        /// Retrieves all entities of type <typeparamref name="T"/> from the mapped table.
        /// </summary>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            return connection.GetList<T>(new { }, transaction, commandTimeout);
        }

        /// <summary>
        /// Executes a paged query using dialect-specific pagination.
        /// </summary>
        public static IEnumerable<T> GetListPaged<T>(this IDbConnection connection, int pageNumber,
            int rowsPerPage, string conditions, string? orderBy, object? parameters = null,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (string.IsNullOrEmpty(Config.PagedListSql))
                throw new NotSupportedException("GetListPaged is not supported for the current SQL dialect.");

            if (pageNumber < 1)
                throw new ArgumentOutOfRangeException(nameof(pageNumber), "Page number must be >= 1.");

            if (rowsPerPage < 1)
                throw new ArgumentOutOfRangeException(nameof(rowsPerPage), "Rows per page must be >= 1.");

            var conv = NewConvention();

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Entity must have at least one [Key] property.");

            var table = conv.GetTableNameEncapsulated(currentType);

            // Choose default ORDER BY if not provided (CLR name, then translated/validated through allowlist)
            if (string.IsNullOrWhiteSpace(orderBy))
                orderBy = idProps.First().Name;

            var allowed = BuildAllowedColumnMap<T>(conv);
            var validated = new List<string>();

            foreach (var token in orderBy.Split(','))
            {
                var t = token.Trim();
                if (t.Length == 0) continue;

                var bits = t.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                if (bits.Length == 0) continue;

                var raw = bits[0];
                var norm = NormalizeIdentifier(raw);

                if (!allowed.TryGetValue(norm, out var encapsulated))
                    throw new ArgumentException($"Invalid ORDER BY column '{raw}' for {typeof(T).Name}.");

                var dir = (bits.Length > 1 ? bits[1] : "ASC").ToUpperInvariant();
                if (dir != "ASC" && dir != "DESC")
                    throw new ArgumentException($"Invalid ORDER BY direction '{dir}'. Use ASC or DESC.");

                validated.Add($"{encapsulated} {dir}");
            }

            if (validated.Count == 0)
                throw new ArgumentException($"ORDER BY could not be resolved for {typeof(T).Name}.");

            orderBy = string.Join(", ", validated);

            var selectCols = new StringBuilder();
            NewBuilder(conv).BuildSelect(selectCols, SqlBuilder.GetScaffoldableProperties<T>());

            var sql = Config.PagedListSql
                .Replace("{SelectColumns}", selectCols.ToString())
                .Replace("{TableName}", table)
                .Replace("{OrderBy}", orderBy)
                .Replace("{PageNumber}", pageNumber.ToString())
                .Replace("{RowsPerPage}", rowsPerPage.ToString());

            if (!string.IsNullOrWhiteSpace(conditions))
            {
                if (!conditions.TrimStart().StartsWith("where", StringComparison.OrdinalIgnoreCase))
                    conditions = " where " + conditions;
            }

            var query = sql.Replace("{WhereClause}", conditions);

            Log(() => $"GetListPaged<{currentType.Name}>: {query}");

            return connection.Query<T>(Cmd(query, parameters, transaction, commandTimeout));
        }

        /// <summary>
        /// Inserts a new entity and returns the generated primary key as an object.
        /// For strongly-typed keys prefer the generic overload.
        /// </summary>
        public static object? Insert<T>(this IDbConnection connection, T entityToInsert,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            return connection.Insert<object, T>(entityToInsert, transaction, commandTimeout);
        }

        /// <summary>
        /// Inserts a new entity and returns the generated primary key as <typeparamref name="TKey"/>.
        /// </summary>
        public static TKey? Insert<TKey, T>(this IDbConnection connection, T entityToInsert,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (entityToInsert is null) throw new ArgumentNullException(nameof(entityToInsert));

            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);
            var idProps = SqlConvention.GetIdProperties(type);

            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Insert<T> requires an entity with a [Key] or Id property.");

            var keyProperty = idProps.First();
            var keyType = keyProperty.PropertyType;
            var isGuidKey = keyType == typeof(Guid);
            var isStringKey = keyType == typeof(string);

            if (isGuidKey)
            {
                var val = (Guid)(keyProperty.GetValue(entityToInsert, null) ?? Guid.Empty);
                if (val == Guid.Empty)
                    keyProperty.SetValue(entityToInsert, SqlConvention.SequentialGuid(), null);
            }
            else if (isStringKey)
            {
                var val = keyProperty.GetValue(entityToInsert, null) as string;
                if (string.IsNullOrWhiteSpace(val))
                    throw new ArgumentException("String key must be supplied before calling Insert when using a string [Key].");
            }

            // Parity: initialize Version to 1 if present and currently 0
            if (SqlConvention.TryGetVersionProperty(type, out var versionProp))
            {
                var current = (long)(versionProp!.GetValue(entityToInsert) ?? 0L);
                if (current == 0L)
                    versionProp.SetValue(entityToInsert, 1L);
            }

            var sbCols = new StringBuilder();
            var sbVals = new StringBuilder();
            builder.BuildInsertParameters<T>(sbCols);
            builder.BuildInsertValues<T>(sbVals);

            var sql = new StringBuilder();
            sql.AppendFormat("insert into {0} ({1}) values ({2})", table, sbCols, sbVals);

            if (!isGuidKey && !isStringKey)
            {
                if (string.IsNullOrEmpty(Config.IdentitySql))
                    throw new NotSupportedException("Identity retrieval SQL is not configured for the current dialect.");

                sql.Append("; ");
                sql.Append(Config.IdentitySql);

                Log(() => $"Insert<{type.Name}>: {sql}");

                var id = connection.ExecuteScalar(Cmd(sql.ToString(), entityToInsert, transaction, commandTimeout));
                if (id == null || id is DBNull) return default;

                WriteIdentityBack(entityToInsert, keyProperty, id);

                return ConvertIdentity<TKey>(id);
            }

            Log(() => $"Insert<{type.Name}>: {sql}");

            connection.Execute(Cmd(sql.ToString(), entityToInsert, transaction, commandTimeout));
            return (TKey?)keyProperty.GetValue(entityToInsert, null);
        }

        /// <summary>
        /// Updates an existing entity identified by its key property (or properties for composite keys).
        /// </summary>
        public static int Update<T>(this IDbConnection connection, T entityToUpdate,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (entityToUpdate is null) throw new ArgumentNullException(nameof(entityToUpdate));

            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Update<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", table);
            builder.BuildUpdateSet(entityToUpdate, sb);
            sb.Append(" where ");

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            Log(() => $"Update<{type.Name}>: {sb}");

            return connection.Execute(Cmd(sb.ToString(), entityToUpdate, transaction, commandTimeout));
        }

        /// <summary>
        /// Updates an entity with optimistic concurrency using a version column.
        /// The version is incremented automatically in the UPDATE statement.
        /// </summary>
        public static int UpdateWithVersion<T>(this IDbConnection connection, T entityToUpdate,
            long expectedVersion, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (entityToUpdate == null) throw new ArgumentNullException(nameof(entityToUpdate));

            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);

            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("UpdateWithVersion<T> requires an entity with a [Key] or Id property.");

            var versionProp = SqlConvention.GetVersionProperty(type)
                ?? throw new ArgumentException(
                    $"{type.Name} must have a public long Version property or a property marked with [Version].");

            var table = conv.GetTableNameEncapsulated(type);
            var versionCol = conv.GetColumnNameEncapsulated(versionProp);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", table);
            int lengthBeforeSet = sb.Length;

            builder.BuildUpdateSet(entityToUpdate, sb);

            bool hasOtherColumns = sb.Length > lengthBeforeSet;

            if (hasOtherColumns)
                sb.AppendFormat(", {0} = {0} + 1", versionCol);
            else
                sb.AppendFormat("{0} = {0} + 1", versionCol);

            sb.Append(" where ");
            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            sb.AppendFormat(" and {0} = @ExpectedVersion", versionCol);

            var p = new DynamicParameters(entityToUpdate);
            p.Add("@ExpectedVersion", expectedVersion);

            Log(() => $"UpdateWithVersion<{type.Name}>: {sb}");

            return connection.Execute(Cmd(sb.ToString(), p, transaction, commandTimeout));
        }

        /// <summary>
        /// Deletes an entity by using its key property values from the passed instance.
        /// </summary>
        public static int Delete<T>(this IDbConnection connection, T entityToDelete,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (entityToDelete is null) throw new ArgumentNullException(nameof(entityToDelete));

            var conv = NewConvention();

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Delete<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            Log(() => $"Delete<{type.Name}>: {sb}");

            return connection.Execute(Cmd(sb.ToString(), entityToDelete, transaction, commandTimeout));
        }

        /// <summary>
        /// Deletes an entity by its primary key value (or composite key values).
        /// </summary>
        public static int Delete<T>(this IDbConnection connection, object id,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (id is null) throw new ArgumentNullException(nameof(id));

            var conv = NewConvention();

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Delete<T> requires an entity with a [Key] or Id property.");

            var dyn = new DynamicParameters();
            if (idProps.Length == 1)
            {
                dyn.Add("@" + idProps[0].Name, id);
            }
            else
            {
                foreach (var p in idProps)
                {
                    var idProp = id.GetType().GetProperty(p.Name);
                    if (idProp == null)
                        throw new ArgumentException($"Missing key property '{p.Name}' on id object for {typeof(T).Name}.");
                    dyn.Add("@" + p.Name, idProp.GetValue(id, null));
                }
            }

            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            Log(() => $"Delete<{type.Name}> by id: {sb}");

            return connection.Execute(Cmd(sb.ToString(), dyn, transaction, commandTimeout));
        }

        /// <summary>
        /// Deletes multiple rows using an anonymous object for equality-based filters.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="whereConditions"/> is null or has no properties, to prevent
        /// accidental full-table deletes. Use the string-conditions overload with conditions = "1=1"
        /// if you intentionally want to delete all rows.
        /// </exception>
        public static int DeleteList<T>(this IDbConnection connection, object? whereConditions,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var whereProps = GetAllProperties(whereConditions)?.ToArray();
            if (whereProps == null || whereProps.Length == 0)
                throw new ArgumentException(
                    $"DeleteList<{typeof(T).Name}> requires at least one filter property to prevent accidental full-table deletes. " +
                    "To delete all rows intentionally, use the string-conditions overload with conditions = \"1=1\".");

            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);
            builder.BuildWhere<T>(sb, whereProps, whereConditions);

            Log(() => $"DeleteList<{type.Name}>: {sb}");

            return connection.Execute(sb.ToString(), whereConditions, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes multiple rows using a raw SQL WHERE fragment with optional parameters.
        /// </summary>
        public static int DeleteList<T>(this IDbConnection connection, string conditions,
            object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (string.IsNullOrWhiteSpace(conditions))
                throw new ArgumentException(
                    $"DeleteList<{typeof(T).Name}> requires at least one filter property to prevent accidental full-table deletes. " +
                    "To delete all rows intentionally, use the string-conditions overload with conditions = \"1=1\".",
                    nameof(conditions));

            var conv = NewConvention();

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0}", table);

            if (!string.IsNullOrWhiteSpace(conditions))
            {
                if (!conditions.TrimStart().StartsWith("where", StringComparison.OrdinalIgnoreCase))
                    sb.Append(" where ");
                else
                    sb.Append(" ");
                sb.Append(conditions);
            }

            Log(() => $"DeleteList<{type.Name}>: {sb}");

            return connection.Execute(Cmd(sb.ToString(), parameters, transaction, commandTimeout));
        }

        /// <summary>
        /// Returns the number of rows that match an optional WHERE clause.
        /// </summary>
        public static int RecordCount<T>(this IDbConnection connection, string conditions = "",
            object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("Select count(1) from {0}", table);

            if (!string.IsNullOrWhiteSpace(conditions))
            {
                if (!conditions.TrimStart().StartsWith("where", StringComparison.OrdinalIgnoreCase))
                    sb.Append(" where ");
                else
                    sb.Append(" ");
                sb.Append(conditions);
            }

            Log(() => $"RecordCount<{type.Name}>: {sb}");

            return connection.ExecuteScalar<int>(Cmd(sb.ToString(), parameters, transaction, commandTimeout));
        }

        // ---- helpers ----

        internal static void WriteIdentityBack<T>(T entity, PropertyInfo keyProperty, object id)
        {
            try
            {
                var keyTargetType = Nullable.GetUnderlyingType(keyProperty.PropertyType) ?? keyProperty.PropertyType;
                var converted = Convert.ChangeType(id, keyTargetType);
                keyProperty.SetValue(entity, converted, null);
            }
            catch (Exception ex)
            {
                throw new InvalidCastException(
                    $"Could not assign identity '{id}' ({id.GetType().FullName}) to {typeof(T).Name}.{keyProperty.Name} ({keyProperty.PropertyType.FullName}).",
                    ex);
            }
        }

        internal static TKey? ConvertIdentity<TKey>(object id)
        {
            var targetType = Nullable.GetUnderlyingType(typeof(TKey)) ?? typeof(TKey);

            try
            {
                if (targetType == typeof(object)) return (TKey)id;
                if (targetType == typeof(long)) return (TKey)(object)Convert.ToInt64(id);
                if (targetType == typeof(int)) return (TKey)(object)Convert.ToInt32(id);
                if (targetType == typeof(short)) return (TKey)(object)Convert.ToInt16(id);

                return (TKey)Convert.ChangeType(id, targetType);
            }
            catch (Exception ex)
            {
                throw new InvalidCastException(
                    $"Could not convert identity value '{id}' ({id.GetType().FullName}) to {typeof(TKey).FullName}.", ex);
            }
        }

        internal static Dictionary<string, string> BuildAllowedColumnMap<T>(SqlConvention conv)
        {
            var key = (typeof(T), Config.Dialect.ToString());
            return ColumnMapCache.GetOrAdd(key, _ =>
            {
                var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

                foreach (var p in SqlBuilder.GetScaffoldableProperties<T>())
                {
                    var raw = conv.GetColumnName(p);
                    if (string.IsNullOrWhiteSpace(raw)) continue;

                    var encapsulated = conv.Encapsulate(raw);

                    // Allow either the raw column name or CLR property name as input
                    map[NormalizeIdentifier(raw)] = encapsulated;
                    map[NormalizeIdentifier(p.Name)] = encapsulated;
                }

                return map;
            });
        }

        internal static string NormalizeIdentifier(string s)
        {
            s = s.Trim();
            var lastDot = s.LastIndexOf('.');
            if (lastDot >= 0 && lastDot < s.Length - 1) s = s.Substring(lastDot + 1);

            if ((s.StartsWith("[") && s.EndsWith("]")) ||
                (s.StartsWith("\"") && s.EndsWith("\"")) ||
                (s.StartsWith("`") && s.EndsWith("`")))
                s = s.Substring(1, s.Length - 2);

            return s.ToLowerInvariant();
        }

        internal static IEnumerable<PropertyInfo>? GetAllProperties(object? obj)
        {
            return obj?.GetType().GetProperties();
        }

        private static CommandDefinition Cmd(string sql, object? param, IDbTransaction? tx, int? timeout) => new(
            commandText: sql,
            parameters: param,
            transaction: tx,
            commandTimeout: timeout,
            commandType: null,
            flags: CommandFlags.Buffered
        );

        private static CommandDefinition StoredProc(string storedProcedureName, object? param, IDbTransaction? tx, int? timeout) => new(
            commandText: storedProcedureName,
            parameters: param,
            transaction: tx,
            commandTimeout: timeout,
            commandType: CommandType.StoredProcedure,
            flags: CommandFlags.Buffered
        );
    }
}