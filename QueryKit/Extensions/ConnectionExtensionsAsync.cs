using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using QueryKit.Sql;

namespace QueryKit.Extensions
{
    /// <summary>
    /// Provides asynchronous CRUD extension methods for <see cref="IDbConnection"/> using Dapper.
    /// </summary>
    public static class ConnectionExtensionsAsync
    {
        private static readonly ConcurrentDictionary<(Type, string),
            Dictionary<string, string>> ColumnMapCache = new();

        internal static void ClearColumnMapCache()
        {
            ColumnMapCache.Clear();
        }

        /// <summary>
        /// Asynchronously retrieves a single entity by its primary key.
        /// </summary>
        public static async Task<T?> GetAsync<T>(this IDbConnection connection, object id,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("GetAsync<T> requires an entity with a [Key] or Id property.");

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
                        throw new ArgumentException(
                            $"Missing key property '{p.Name}' on id object for {typeof(T).Name}.");
                    dyn.Add("@" + p.Name, val.GetValue(id, null));
                }
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetAsync<{currentType.Name}>: {sb} with Id: {id}");

            var result =
                await connection.QueryAsync<T>(Cmd(sb.ToString(), dyn, transaction, commandTimeout, cancellationToken));
            return result.FirstOrDefault();
        }

        /// <summary>
        /// Asynchronously executes a stored procedure and maps the results to a list of entities.
        /// </summary>
        public static async Task<IList<T>> ExecuteStoredProcedureAsync<T>(this IDbConnection connection,
            string storedProcedureName, object? parameters = null,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var result = await connection.QueryAsync<T>(StoredProc(storedProcedureName,
                parameters, transaction, commandTimeout, cancellationToken));
            return result.ToList();
        }

        /// <summary>
        /// Asynchronously queries entities using an anonymous object for equality-based filters.
        /// </summary>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, object? whereConditions,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default,
            params (Expression<Func<T, object>> Body, bool Descending)[] orderBy)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

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

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetListAsync<{currentType.Name}>: {sb}");

            return connection.QueryAsync<T>(Cmd(sb.ToString(), whereConditions, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously queries entities using a raw SQL <c>WHERE</c> fragment with optional parameters.
        /// </summary>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, string conditions,
            object? parameters = null, string? orderBy = null, IDbTransaction? transaction = null,
            int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

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
                var parts = orderBy?.Split(',');
                if (parts != null)
                {
                    for (int i = 0; i < parts.Length; i++)
                    {
                        var token = parts[i].Trim();
                        if (string.IsNullOrEmpty(token)) continue;

                        string?[] bits = token.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        if (bits.Length == 0) continue;

                        var rawCol = bits[0]!;
                        var norm = NormalizeIdentifier(rawCol);
                        if (!allowed.TryGetValue(norm, out var encapsulated))
                            throw new ArgumentException("Invalid ORDER BY column '" + rawCol + "' for " +
                                                        currentType.Name + ".");

                        var dir = (bits.Length > 1 ? bits[1] : "ASC")?.ToUpperInvariant();
                        if (dir != "ASC" && dir != "DESC")
                            throw new ArgumentException("Invalid ORDER BY direction '" + dir + "'. Use ASC or DESC.");

                        validated.Add(encapsulated + " " + dir);
                    }
                }

                if (validated.Count > 0)
                    sb.Append(" order by ").Append(string.Join(", ", validated));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetListAsync<{currentType.Name}>: {sb}");

            return connection.QueryAsync<T>(Cmd(sb.ToString(), parameters, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously retrieves all entities from the mapped table.
        /// </summary>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection,
            CancellationToken cancellationToken = default)
        {
            return connection.GetListAsync<T>(new { }, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a paged query using dialect-specific pagination.
        /// </summary>
        /// <exception cref="NotSupportedException">Thrown when paging is not supported for the current dialect.</exception>
        public static Task<IEnumerable<T>> GetListPagedAsync<T>(this IDbConnection connection, int pageNumber,
            int rowsPerPage, string conditions, string? orderBy, object? parameters = null,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(ConnectionExtensions.Config.PagedListSql))
                throw new NotSupportedException("GetListPagedAsync is not supported for the current SQL dialect.");

            if (pageNumber < 1)
                throw new ArgumentOutOfRangeException(nameof(pageNumber), "Page number must be >= 1.");

            if (rowsPerPage < 1)
                throw new ArgumentOutOfRangeException(nameof(rowsPerPage), "Rows per page must be >= 1.");

            var conv = ConnectionExtensions.NewConvention();

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Entity must have at least one [Key] property.");

            var table = conv.GetTableNameEncapsulated(currentType);

            // Choose default ORDER BY if not provided
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                // Use CLR name here, but then translate it through the allowlist below
                orderBy = idProps.First().Name;
            }

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
                {
                    throw new ArgumentException($"Invalid ORDER BY column '{raw}' for {typeof(T).Name}.");
                }

                var dir = (bits.Length > 1 ? bits[1] : "ASC").ToUpperInvariant();
                if (dir != "ASC" && dir != "DESC")
                {
                    throw new ArgumentException($"Invalid ORDER BY direction '{dir}'. Use ASC or DESC.");
                }

                validated.Add($"{encapsulated} {dir}");
            }

            if (validated.Count == 0)
            {
                throw new ArgumentException($"ORDER BY could not be resolved for {typeof(T).Name}.");
            }

            orderBy = string.Join(", ", validated);

            var selectCols = new StringBuilder();
            ConnectionExtensions.NewBuilder(conv).BuildSelect(selectCols, SqlBuilder.GetScaffoldableProperties<T>());

            var sql = ConnectionExtensions.Config.PagedListSql
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

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetListPagedAsync<{currentType.Name}>: {query}");

            return connection.QueryAsync<T>(Cmd(query, parameters, transaction, commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously inserts a new entity and returns the generated primary key as an <see cref="object"/>.
        /// For strongly-typed keys prefer the generic overload.
        /// </summary>
        public static Task<object?> InsertAsync<T>(this IDbConnection connection, T entityToInsert,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            return connection.InsertAsync<object, T>(entityToInsert, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Asynchronously inserts a new entity and returns the generated primary key as <typeparamref name="TKey"/>.
        /// </summary>
        public static async Task<TKey?> InsertAsync<TKey, T>(this IDbConnection connection, T entityToInsert,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);
            var idProps = SqlConvention.GetIdProperties(type);

            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("InsertAsync<T> requires an entity with a [Key] or Id property.");

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
                    throw new ArgumentException(
                        "String key must be supplied before calling InsertAsync when using a string [Key].");
            }

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
                if (string.IsNullOrEmpty(ConnectionExtensions.Config.IdentitySql))
                    throw new NotSupportedException(
                        "Identity retrieval SQL is not configured for the current dialect.");

                sql.Append("; ");
                sql.Append(ConnectionExtensions.Config.IdentitySql);

                if (Debugger.IsAttached)
                    Trace.WriteLine($"InsertAsync<{type.Name}>: {sql}");

                var id = await connection.ExecuteScalarAsync(Cmd(sql.ToString(), entityToInsert,
                    transaction, commandTimeout, cancellationToken));
                if (id == null || id is DBNull) return default;

                var targetType = Nullable.GetUnderlyingType(typeof(TKey)) ?? typeof(TKey);

                try
                {
                    // Most identity keys are integral
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

            if (Debugger.IsAttached)
                Trace.WriteLine($"InsertAsync<{type.Name}>: {sql}");

            await connection.ExecuteAsync(Cmd(sql.ToString(), entityToInsert, transaction, commandTimeout,
                cancellationToken));
            return (TKey?)keyProperty.GetValue(entityToInsert, null);
        }

        /// <summary>
        /// Asynchronously updates an existing entity identified by its key property.
        /// </summary>
        public static Task<int> UpdateAsync<T>(this IDbConnection connection, T entityToUpdate,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("UpdateAsync<T> requires an entity with a [Key] or Id property.");

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

            if (Debugger.IsAttached)
                Trace.WriteLine($"UpdateAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), entityToUpdate, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously updates an entity with optimistic concurrency using a version column.
        /// The version is incremented automatically in the UPDATE statement.
        /// </summary>
        public static Task<int> UpdateWithVersionAsync<T>(
            this IDbConnection connection,
            T entityToUpdate,
            long expectedVersion,
            IDbTransaction? transaction = null,
            int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            if (entityToUpdate == null) throw new ArgumentNullException(nameof(entityToUpdate));

            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var type = typeof(T);

            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException(
                    "UpdateWithVersionAsync<T> requires an entity with a [Key] or Id property.");

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
            {
                sb.AppendFormat(", {0} = {0} + 1", versionCol);
            }
            else
            {
                // Entity has only key/version columns. We still allow the update so callers can use
                // this to perform a "version bump only" operation (e.g. optimistic lock heartbeat).
                sb.AppendFormat("{0} = {0} + 1", versionCol);
            }

            sb.Append(" where ");
            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            // Concurrency gate
            sb.AppendFormat(" and {0} = @ExpectedVersion", versionCol);

            var p = new DynamicParameters(entityToUpdate);
            p.Add("@ExpectedVersion", expectedVersion);

            if (Debugger.IsAttached)
                Trace.WriteLine($"UpdateWithVersionAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), p, transaction, commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes an entity by using its key property values from the passed instance.
        /// </summary>
        public static Task<int> DeleteAsync<T>(this IDbConnection connection, T entityToDelete,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("DeleteAsync<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);
            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnNameEncapsulated(idProps[i]), idProps[i].Name);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), entityToDelete, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes an entity by its primary key value (or composite key values).
        /// </summary>
        public static Task<int> DeleteAsync<T>(this IDbConnection connection, object id,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("DeleteAsync<T> requires an entity with a [Key] or Id property.");

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
                        throw new ArgumentException(
                            $"Missing key property '{p.Name}' on id object for {typeof(T).Name}.");
                    var val = idProp.GetValue(id, null);
                    dyn.Add("@" + p.Name, val);
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

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteAsync<{type.Name}> by id: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), dyn, transaction, commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes multiple rows using an anonymous object for equality-based filters.
        /// </summary>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="whereConditions"/> is null or has no properties, to prevent
        /// accidental full-table deletes. Use <see cref="DeleteListAsync{T}(IDbConnection, string, object, IDbTransaction, int?, CancellationToken)"/>
        /// with an explicit <c>WHERE</c> clause if you intentionally want to delete all rows.
        /// </exception>
        public static Task<int> DeleteListAsync<T>(this IDbConnection connection, object? whereConditions,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var whereProps = GetAllProperties(whereConditions)?.ToArray();
            if (whereProps == null || whereProps.Length == 0)
                throw new ArgumentException(
                    $"DeleteListAsync<{typeof(T).Name}> requires at least one filter property to prevent accidental full-table deletes. " +
                    "To delete all rows intentionally, use the string-conditions overload with conditions = \"1=1\".");

            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);
            builder.BuildWhere<T>(sb, whereProps, whereConditions);

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteListAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), whereConditions, transaction,
                commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes multiple rows using a raw SQL <c>WHERE</c> fragment with optional parameters.
        /// </summary>
        public static Task<int> DeleteListAsync<T>(this IDbConnection connection, string conditions,
            object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrWhiteSpace(conditions))
                throw new ArgumentException(
                    $"DeleteListAsync<{typeof(T).Name}> requires at least one filter property to prevent accidental full-table deletes. " +
                    "To delete all rows intentionally, use the string-conditions overload with conditions = \"1=1\".",
                    nameof(conditions));

            var conv = ConnectionExtensions.NewConvention();

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0}", table);

            if (!conditions.TrimStart().StartsWith("where", StringComparison.OrdinalIgnoreCase))
                sb.Append(" where ");
            else
                sb.Append(" ");

            sb.Append(conditions);

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteListAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), parameters, transaction, commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously returns the number of rows that match an optional <c>WHERE</c> clause.
        /// </summary>
        public static Task<int> RecordCountAsync<T>(this IDbConnection connection, string conditions = "",
            object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();

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

            if (Debugger.IsAttached)
                Trace.WriteLine($"RecordCountAsync<{type.Name}>: {sb}");

            return connection.ExecuteScalarAsync<int>(Cmd(sb.ToString(), parameters, transaction, commandTimeout,
                cancellationToken));
        }

        private static Dictionary<string, string> BuildAllowedColumnMap<T>(SqlConvention conv)
        {
            var key = (typeof(T), ConnectionExtensions.Config.Dialect.ToString());
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

        private static string NormalizeIdentifier(string s)
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

        private static IEnumerable<PropertyInfo>? GetAllProperties(object? obj)
        {
            return obj?.GetType().GetProperties();
        }

        private static CommandDefinition Cmd(string sql, object? param, IDbTransaction? tx,
            int? timeout, CancellationToken ct) => new(
            commandText: sql,
            parameters: param,
            transaction: tx,
            commandTimeout: timeout,
            commandType: null,
            flags: CommandFlags.Buffered,
            cancellationToken: ct
        );

        private static CommandDefinition StoredProc(string storedProcedureName, object? param, IDbTransaction? tx,
            int? timeout, CancellationToken ct) => new(
            commandText: storedProcedureName,
            parameters: param,
            transaction: tx,
            commandTimeout: timeout,
            commandType: CommandType.StoredProcedure,
            flags: CommandFlags.Buffered,
            cancellationToken: ct
        );
    }
}