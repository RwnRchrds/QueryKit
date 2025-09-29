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
    /// </summary>
    public static class ConnectionExtensions
    {
        /// <summary>
        /// Gets the active SQL dialect configuration used by QueryKit.
        /// </summary>
        public static DialectConfig Config { get; private set; } = DialectConfig.Create(Dialect.SQLServer);

        /// <summary>
        /// Sets the SQL dialect used for identifier quoting, identity retrieval, and paging SQL.
        /// </summary>
        /// <param name="dialect">The dialect to use (e.g., <see cref="Dialect.SQLServer"/>).</param>
        public static void UseDialect(Dialect dialect)
        {
            Config = DialectConfig.Create(dialect);
            ColumnMapCache.Clear();
            ConnectionExtensionsAsync.ClearColumnMapCache();
        }

        internal static SqlConvention NewConvention() =>
            new SqlConvention(Config, new TableNameResolver(), new ColumnNameResolver());

        internal static SqlBuilder NewBuilder(SqlConvention conv) =>
            new SqlBuilder(conv);

        // cache: normalized name -> encapsulated column name for T + dialect
        private static readonly ConcurrentDictionary<(Type, string), Dictionary<string, string>> ColumnMapCache = new();

        /// <summary>
        /// Builds an ascending order by expression tuple for use with GetList.
        /// </summary>
        public static (Expression<Func<T, object>>, bool) OrderByAscending<T>(Expression<Func<T, object>> e) => (e, false);

        /// <summary>
        /// Builds a descending order by expression tuple for use with GetList.
        /// </summary>
        public static (Expression<Func<T, object>>, bool) OrderByDescending<T>(Expression<Func<T, object>> e) => (e, true);

        /// <summary>
        /// Retrieves a single entity of type <typeparamref name="T"/> by its primary key.
        /// </summary>
        public static T? Get<T>(this IDbConnection connection, object id, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Get<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableName(currentType);

            var sb = new StringBuilder();
            sb.Append("Select ");
            builder.BuildSelect(sb, SqlBuilder.GetScaffoldableProperties<T>());
            sb.AppendFormat(" from {0} where ", table);

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnName(idProps[i]), idProps[i].Name);
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
                    var val = id.GetType().GetProperty(p.Name)!.GetValue(id, null);
                    dyn.Add("@" + p.Name, val);
                }
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Get<{currentType.Name}>: {sb} with Id: {id}");

            return connection.Query<T>(sb.ToString(), dyn, transaction, buffered: true, commandTimeout).FirstOrDefault();
        }

        /// <summary>
        /// Executes a stored procedure and maps the results to a list of <typeparamref name="T"/>.
        /// </summary>
        public static IList<T> ExecuteStoredProcedure<T>(this IDbConnection connection,
            string storedProcedureName, object? parameters = null,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var result = connection.Query<T>(storedProcedureName, parameters, transaction, buffered: true,
                commandTimeout, CommandType.StoredProcedure);
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
            var table = conv.GetTableName(currentType);

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

                    var col = conv.GetColumnName(prop);
                    if (string.IsNullOrEmpty(col))
                        throw new ArgumentException($"Property '{prop.Name}' is not mapped for {typeof(T).Name}.");
                    cols.Add(desc ? $"{col} DESC" : $"{col} ASC");
                }

                sb.Append(" order by ").Append(string.Join(", ", cols));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetList<{currentType.Name}>: {sb}");

            return connection.Query<T>(sb.ToString(), whereConditions, transaction, buffered: true, commandTimeout);
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
            var table = conv.GetTableName(currentType);

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
                var allowed = BuildAllowedColumnMap<T>(conv);
                var validated = new List<string>();
                var parts = orderBy!.Split(',');

                foreach (var rawToken in parts)
                {
                    var token = rawToken.Trim();
                    if (token.Length == 0) continue;

                    var bits = token.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    var rawCol = bits[0];
                    var norm = NormalizeIdentifier(rawCol);
                    if (!allowed.TryGetValue(norm, out var encapsulated))
                        throw new ArgumentException($"Invalid ORDER BY column '{rawCol}' for {currentType.Name}.");

                    var dir = (bits.Length > 1 ? bits[1] : "ASC").ToUpperInvariant();
                    if (dir != "ASC" && dir != "DESC")
                        throw new ArgumentException($"Invalid ORDER BY direction '{dir}'. Use ASC or DESC.");

                    validated.Add($"{encapsulated} {dir}");
                }

                if (validated.Count > 0)
                    sb.Append(" order by ").Append(string.Join(", ", validated));
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetList<{currentType.Name}>: {sb}");

            return connection.Query<T>(sb.ToString(), parameters, transaction, buffered: true, commandTimeout);
        }

        /// <summary>
        /// Retrieves all entities of type <typeparamref name="T"/> from the mapped table.
        /// </summary>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection)
        {
            return connection.GetList<T>(new { });
        }

        /// <summary>
        /// Executes a paged query using dialect-specific pagination.
        /// </summary>
        public static IEnumerable<T> GetListPaged<T>(this IDbConnection connection, int pageNumber, int rowsPerPage, string conditions, string? orderBy, object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            if (string.IsNullOrEmpty(Config.PagedListSql))
                throw new NotSupportedException("GetListPaged is not supported for the current SQL dialect.");

            if (pageNumber < 1)
                throw new ArgumentOutOfRangeException(nameof(pageNumber), "Page number must be >= 1.");

            var conv = NewConvention();

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Entity must have at least one [Key] property.");

            var table = conv.GetTableName(currentType);

            // default to PK when not provided
            if (string.IsNullOrWhiteSpace(orderBy))
            {
                orderBy = conv.GetColumnName(idProps.First());
                if (string.IsNullOrEmpty(orderBy))
                    throw new ArgumentException($"Primary key for {typeof(T).Name} is not mapped to a column.");
            }
            else
            {
                // validate and normalize
                var allowed = BuildAllowedColumnMap<T>(conv);
                var validated = new List<string>();
                foreach (var token in orderBy!.Split(','))
                {
                    var t = token.Trim();
                    if (t.Length == 0) continue;

                    var bits = t.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    var raw = bits[0];
                    var norm = NormalizeIdentifier(raw);
                    if (!allowed.TryGetValue(norm, out var encapsulated))
                        throw new ArgumentException($"Invalid ORDER BY column '{raw}' for {currentType.Name}.");

                    var dir = (bits.Length > 1 ? bits[1] : "ASC").ToUpperInvariant();
                    if (dir != "ASC" && dir != "DESC")
                        throw new ArgumentException($"Invalid ORDER BY direction '{dir}'. Use ASC or DESC.");

                    validated.Add($"{encapsulated} {dir}");
                }

                orderBy = string.Join(", ", validated);
            }

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

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetListPaged<{currentType.Name}>: {query}");

            return connection.Query<T>(query, parameters, transaction, buffered: true, commandTimeout);
        }

        /// <summary>
        /// Inserts a new entity and returns the generated primary key as an object.
        /// </summary>
        public static object? Insert<T>(this IDbConnection connection, T entityToInsert, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            return Insert<object, T>(connection, entityToInsert, transaction, commandTimeout);
        }

        /// <summary>
        /// Inserts a new entity and returns the generated primary key as <typeparamref name="TKey"/>.
        /// </summary>
        public static TKey? Insert<TKey, T>(this IDbConnection connection, T entityToInsert, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableName(type);
            var idProps = SqlConvention.GetIdProperties(type);

            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Insert<T> requires an entity with a [Key] or Id property.");

            var keyProperty = idProps.First();
            var keyType = keyProperty.PropertyType;
            var isGuidKey = keyType == typeof(Guid);
            var isStringKey = keyType == typeof(string);

            // Pre-generate Guid/string keys if needed
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

            var sbCols = new StringBuilder();
            var sbVals = new StringBuilder();
            builder.BuildInsertParameters<T>(sbCols);
            builder.BuildInsertValues<T>(sbVals);

            var sql = new StringBuilder();
            sql.AppendFormat("insert into {0} ({1}) values ({2})", table, sbCols, sbVals);

            // Identity retrieval for numeric identity keys
            if (!isGuidKey && !isStringKey)
            {
                if (string.IsNullOrEmpty(Config.IdentitySql))
                    throw new NotSupportedException("Identity retrieval SQL is not configured for the current dialect.");

                sql.Append("; ");
                sql.Append(Config.IdentitySql);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Insert<{type.Name}>: {sql}");

            if (!isGuidKey && !isStringKey)
            {
                var id = connection.ExecuteScalar(sql.ToString(), entityToInsert, transaction, commandTimeout);
                if (id == null || id is DBNull) return default;
                return (TKey)Convert.ChangeType(id, typeof(TKey));
            }
            else
            {
                connection.Execute(sql.ToString(), entityToInsert, transaction, commandTimeout);
                return (TKey?)keyProperty.GetValue(entityToInsert, null);
            }
        }

        /// <summary>
        /// Updates an existing entity identified by its key property (or properties for composite keys).
        /// </summary>
        public static int Update<T>(this IDbConnection connection, T entityToUpdate, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Update<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("update {0} set ", table);
            builder.BuildUpdateSet(entityToUpdate, sb);
            sb.Append(" where ");

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnName(idProps[i]), idProps[i].Name);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Update<{type.Name}>: {sb}");

            return connection.Execute(sb.ToString(), entityToUpdate, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes an entity by using its key property values from the passed instance.
        /// </summary>
        public static int Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Delete<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);
            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnName(idProps[i]), idProps[i].Name);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Delete<{type.Name}>: {sb}");

            return connection.Execute(sb.ToString(), entityToDelete, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes an entity by its primary key value (or composite key values).
        /// </summary>
        public static int Delete<T>(this IDbConnection connection, object id, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
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
                    var val = id.GetType().GetProperty(p.Name)!.GetValue(id, null);
                    dyn.Add("@" + p.Name, val);
                }
            }

            var table = conv.GetTableName(type);
            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnName(idProps[i]), idProps[i].Name);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"Delete<{type.Name}> by id: {sb}");

            return connection.Execute(sb.ToString(), dyn, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes multiple rows using an anonymous object for equality-based filters.
        /// </summary>
        public static int DeleteList<T>(this IDbConnection connection, object? whereConditions, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();
            var builder = NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableName(type);
            var whereProps = GetAllProperties(whereConditions)?.ToArray();

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0}", table);

            if (whereProps != null && whereProps.Any())
            {
                sb.Append(" where ");
                builder.BuildWhere<T>(sb, whereProps, whereConditions);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteList<{type.Name}>: {sb}");

            return connection.Execute(sb.ToString(), whereConditions, transaction, commandTimeout);
        }

        /// <summary>
        /// Deletes multiple rows using a raw SQL WHERE fragment with optional parameters.
        /// </summary>
        public static int DeleteList<T>(this IDbConnection connection, string conditions, object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();

            var type = typeof(T);
            var table = conv.GetTableName(type);

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

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteList<{type.Name}>: {sb}");

            return connection.Execute(sb.ToString(), parameters, transaction, commandTimeout);
        }

        /// <summary>
        /// Returns the number of rows that match an optional WHERE clause.
        /// </summary>
        public static int RecordCount<T>(this IDbConnection connection, string conditions = "", object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var conv = NewConvention();

            var type = typeof(T);
            var table = conv.GetTableName(type);

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
                Trace.WriteLine($"RecordCount<{type.Name}>: {sb}");

            return connection.ExecuteScalar<int>(sb.ToString(), parameters, transaction, commandTimeout);
        }

        // ---- helpers ----

        private static Dictionary<string, string> BuildAllowedColumnMap<T>(SqlConvention conv)
        {
            var key = (typeof(T), Config.Dialect.ToString());
            return ColumnMapCache.GetOrAdd(key, _ =>
            {
                var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                foreach (var p in SqlBuilder.GetScaffoldableProperties<T>())
                {
                    var col = conv.GetColumnName(p);
                    if (!string.IsNullOrEmpty(col))
                    {
                        map[NormalizeIdentifier(col)] = col;
                        map[NormalizeIdentifier(p.Name)] = col;
                    } 
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
    }
}