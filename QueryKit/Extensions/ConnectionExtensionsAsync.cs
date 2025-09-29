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
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="id">The primary key value. For composite keys, provide an object with matching property names.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken"></param>
        /// <returns>A task producing the entity if found; otherwise <c>null</c>.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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
                    var val = id.GetType().GetProperty(p.Name);
                    if (val == null)
                    {
                        throw new ArgumentException($"Missing key property '{p.Name}' on id object for {typeof(T).Name}.");
                    }
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
        /// <param name="connection">The database connection.</param>
        /// <param name="storedProcedureName">The name of the stored procedure to execute.</param>
        /// <param name="parameters"></param>
        /// <param name="transaction"></param>
        /// <param name="commandTimeout"></param>
        /// <param name="cancellationToken"></param>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <returns></returns>
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
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="whereConditions">Anonymous object of filter properties/values.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <param name="orderBy">A function of columns to sort by.</param>
        /// <returns>A task producing the sequence of matching entities.</returns>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, object? whereConditions,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default,
            params (Expression<Func<T,object>> Body, bool Descending)[] orderBy)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

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
                Trace.WriteLine($"GetListAsync<{currentType.Name}>: {sb}");

            return connection.QueryAsync<T>(Cmd(sb.ToString(), whereConditions, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously queries entities using a raw SQL <c>WHERE</c> fragment with optional parameters.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="orderBy">Columns to order by.</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the sequence of matching entities.</returns>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection, string conditions,
            object? parameters = null, string? orderBy = null, IDbTransaction? transaction = null,
            int? commandTimeout = null, CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

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
                // Validate and normalize the ORDER BY against T's columns
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
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the sequence of all entities.</returns>
        public static Task<IEnumerable<T>> GetListAsync<T>(this IDbConnection connection,
            CancellationToken cancellationToken = default)
        {
            return connection.GetListAsync<T>(new { }, cancellationToken: cancellationToken);
        }

        /// <summary>
        /// Asynchronously executes a paged query using dialect-specific pagination.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="pageNumber">1-based page number.</param>
        /// <param name="rowsPerPage">Number of rows per page.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="orderBy">The <c>ORDER BY</c> clause (e.g., <c>"LastName asc"</c>).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the requested page of results.</returns>
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

            var conv = ConnectionExtensions.NewConvention();

            var currentType = typeof(T);
            var idProps = SqlConvention.GetIdProperties(currentType);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("Entity must have at least one [Key] property.");

            var table = conv.GetTableName(currentType);

            if (string.IsNullOrWhiteSpace(orderBy))
            {
                orderBy = conv.GetColumnName(idProps.First());
                if (string.IsNullOrEmpty(orderBy))
                    throw new ArgumentException($"Primary key for {typeof(T).Name} is not mapped to a column.");
            }
            else
            {
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

                orderBy = string.Join(", ", validated);
            }

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
        /// <typeparam name="T">The entity type to insert.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToInsert">The entity instance to insert.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the generated primary key value.</returns>
        public static Task<object?> InsertAsync<T>(this IDbConnection connection, T entityToInsert,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            return connection.InsertAsync<object, T>(entityToInsert, transaction, commandTimeout, cancellationToken);
        }

        /// <summary>
        /// Asynchronously inserts a new entity and returns the generated primary key as <typeparamref name="TKey"/>.
        /// </summary>
        /// <typeparam name="TKey">The key type (e.g., <see cref="int"/>, <see cref="long"/>, <see cref="Guid"/>, <see cref="string"/>).</typeparam>
        /// <typeparam name="T">The entity type to insert.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToInsert">The entity instance to insert.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the generated primary key value.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located or when a required string key is not provided.</exception>
        public static async Task<TKey?> InsertAsync<TKey, T>(this IDbConnection connection, T entityToInsert,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableName(type);
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
                return (TKey)Convert.ChangeType(id, typeof(TKey));
            }
            else
            {
                if (Debugger.IsAttached)
                    Trace.WriteLine($"InsertAsync<{type.Name}>: {sql}");

                await connection.ExecuteAsync(Cmd(sql.ToString(), entityToInsert, transaction, commandTimeout,
                    cancellationToken));
                return (TKey?)keyProperty.GetValue(entityToInsert, null);
            }
        }

        /// <summary>
        /// Asynchronously updates an existing entity identified by its key property.
        /// </summary>
        /// <typeparam name="T">The entity type to update.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToUpdate">The entity instance with updated values.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the number of rows affected.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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
                Trace.WriteLine($"UpdateAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), entityToUpdate, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes an entity by using its key property values from the passed instance.
        /// </summary>
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToDelete">The entity instance whose key values will be used.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the number of rows affected.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
        public static Task<int> DeleteAsync<T>(this IDbConnection connection, T entityToDelete,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();

            var type = typeof(T);
            var idProps = SqlConvention.GetIdProperties(type);
            if (idProps == null || idProps.Length == 0)
                throw new ArgumentException("DeleteAsync<T> requires an entity with a [Key] or Id property.");

            var table = conv.GetTableName(type);

            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);
            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnName(idProps[i]), idProps[i].Name);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), entityToDelete, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes an entity by its primary key value (or composite key values).
        /// </summary>
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="id">The key value. For composite keys, supply an object with matching property names.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the number of rows affected.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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

            var table = conv.GetTableName(type);
            var sb = new StringBuilder();
            sb.AppendFormat("delete from {0} where ", table);

            for (int i = 0; i < idProps.Length; i++)
            {
                if (i > 0) sb.Append(" and ");
                sb.AppendFormat("{0} = @{1}", conv.GetColumnName(idProps[i]), idProps[i].Name);
            }

            if (Debugger.IsAttached)
                Trace.WriteLine($"DeleteAsync<{type.Name}> by id: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), dyn, transaction, commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes multiple rows using an anonymous object for equality-based filters.
        /// </summary>
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="whereConditions">Anonymous object of filter properties/values.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the number of rows affected.</returns>
        public static Task<int> DeleteListAsync<T>(this IDbConnection connection, object? whereConditions,
            IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

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
                Trace.WriteLine($"DeleteListAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), whereConditions, transaction,
                commandTimeout, cancellationToken));
        }

        /// <summary>
        /// Asynchronously deletes multiple rows using a raw SQL <c>WHERE</c> fragment with optional parameters.
        /// </summary>
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the number of rows affected.</returns>
        public static Task<int> DeleteListAsync<T>(this IDbConnection connection, string conditions,
            object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();

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
                Trace.WriteLine($"DeleteListAsync<{type.Name}>: {sb}");

            return connection.ExecuteAsync(Cmd(sb.ToString(), parameters, transaction, commandTimeout,
                cancellationToken));
        }

        /// <summary>
        /// Asynchronously returns the number of rows that match an optional <c>WHERE</c> clause.
        /// </summary>
        /// <typeparam name="T">The entity type to count.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <param name="cancellationToken">Optional cancellation token.</param>
        /// <returns>A task producing the number of matching rows.</returns>
        public static Task<int> RecordCountAsync<T>(this IDbConnection connection, string conditions = "",
            object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null,
            CancellationToken cancellationToken = default)
        {
            var conv = ConnectionExtensions.NewConvention();

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

        private static CommandDefinition Cmd(string sql, object? param, IDbTransaction? tx,
            int? timeout, CancellationToken ct) => new CommandDefinition(
            commandText: sql,
            parameters: param,
            transaction: tx,
            commandTimeout: timeout,
            commandType: null,
            flags: CommandFlags.Buffered,
            cancellationToken: ct
        );

        private static CommandDefinition StoredProc(string storedProcedureName, object? param, IDbTransaction? tx,
            int? timeout, CancellationToken ct) => new CommandDefinition(
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