using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
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
        public static void UseDialect(Dialect dialect) => Config = DialectConfig.Create(dialect);

        private static SqlConvention NewConvention() =>
            new SqlConvention(Config, new TableNameResolver(), new ColumnNameResolver());

        private static SqlBuilder NewBuilder(SqlConvention conv) =>
            new SqlBuilder(conv);

        /// <summary>
        /// Retrieves a single entity of type <typeparamref name="T"/> by its primary key.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="id">The primary key value. For composite keys, provide an object with matching property names.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The entity if found; otherwise <c>null</c>.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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
        /// <param name="connection">The database connection.</param>
        /// <param name="storedProcedureName">The name of the stored procedure to execute.</param>
        /// <param name="parameters">The parameters to pass, if any.</param>
        /// <param name="transaction">The transaction to use, if any.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <returns></returns>
        public static IList<T> ExecuteStoredProcedure<T>(this IDbConnection connection,
            string storedProcedureName, object? parameters = null,
            IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            var result = connection.Query<T>(storedProcedureName, parameters, transaction, buffered: true,
                commandTimeout, CommandType.StoredProcedure);
            return result.ToList();
        }

        /// <summary>
        /// Queries entities of type <typeparamref name="T"/> using an anonymous object for equality-based filters.
        /// Each property is translated to <c>Column = @Param</c>.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="whereConditions">Anonymous object of filter properties/values.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>An enumerable of matching entities.</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, object? whereConditions, IDbTransaction? transaction = null, int? commandTimeout = null)
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

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetList<{currentType.Name}>: {sb}");

            return connection.Query<T>(sb.ToString(), whereConditions, transaction, buffered: true, commandTimeout);
        }

        /// <summary>
        /// Queries entities of type <typeparamref name="T"/> using a raw SQL <c>WHERE</c> fragment with optional parameters.
        /// The fragment may include or omit the <c>WHERE</c> keyword.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>An enumerable of matching entities.</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection, string conditions, object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
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

            if (Debugger.IsAttached)
                Trace.WriteLine($"GetList<{currentType.Name}>: {sb}");

            return connection.Query<T>(sb.ToString(), parameters, transaction, buffered: true, commandTimeout);
        }

        /// <summary>
        /// Retrieves all entities of type <typeparamref name="T"/> from the mapped table.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <returns>An enumerable of all entities.</returns>
        public static IEnumerable<T> GetList<T>(this IDbConnection connection)
        {
            return connection.GetList<T>(new { });
        }

        /// <summary>
        /// Executes a paged query using dialect-specific pagination.
        /// </summary>
        /// <typeparam name="T">The entity type to map results to.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="pageNumber">1-based page number.</param>
        /// <param name="rowsPerPage">Number of rows per page.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="orderby">The <c>ORDER BY</c> clause (e.g., <c>"LastName asc, FirstName asc"</c>).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>An enumerable containing the requested page of results.</returns>
        /// <exception cref="NotSupportedException">Thrown when paging is not supported for the current dialect.</exception>
        public static IEnumerable<T> GetListPaged<T>(this IDbConnection connection, int pageNumber, int rowsPerPage, string conditions, string orderby, object? parameters = null, IDbTransaction? transaction = null, int? commandTimeout = null)
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

            if (string.IsNullOrWhiteSpace(orderby))
                orderby = conv.GetColumnName(idProps.First());

            var selectCols = new StringBuilder();
            NewBuilder(conv).BuildSelect(selectCols, SqlBuilder.GetScaffoldableProperties<T>());

            var sql = Config.PagedListSql
                .Replace("{SelectColumns}", selectCols.ToString())
                .Replace("{TableName}", table)
                .Replace("{OrderBy}", orderby)
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
        /// Inserts a new entity and returns the generated primary key as an <see cref="object"/>.
        /// For strongly-typed keys prefer the generic overload.
        /// </summary>
        /// <typeparam name="T">The entity type to insert.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToInsert">The entity instance to insert.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The generated primary key value.</returns>
        public static object? Insert<T>(this IDbConnection connection, T entityToInsert, IDbTransaction? transaction = null, int? commandTimeout = null)
        {
            return Insert<object, T>(connection, entityToInsert, transaction, commandTimeout);
        }

        /// <summary>
        /// Inserts a new entity and returns the generated primary key as <typeparamref name="TKey"/>.
        /// </summary>
        /// <typeparam name="TKey">The key type (e.g., <see cref="int"/>, <see cref="long"/>, <see cref="Guid"/>, <see cref="string"/>).</typeparam>
        /// <typeparam name="T">The entity type to insert.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToInsert">The entity instance to insert.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The generated primary key value.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located or when a required string key is not provided.</exception>
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
                var val = (Guid) (keyProperty.GetValue(entityToInsert, null) ?? Guid.Empty);
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
                // identity case: returns a scalar (long/decimal/etc.) and we map/convert to TKey
                var id = connection.ExecuteScalar(sql.ToString(), entityToInsert, transaction, commandTimeout);
                if (id == null || id is DBNull) return default;
                return (TKey)Convert.ChangeType(id, typeof(TKey));
            }
            else
            {
                // Guid/string key already set on the entity; just execute
                connection.Execute(sql.ToString(), entityToInsert, transaction, commandTimeout);
                return (TKey?)keyProperty.GetValue(entityToInsert, null);
            }
        }

        /// <summary>
        /// Updates an existing entity identified by its key property (or properties for composite keys).
        /// Respects mapping attributes such as <c>[IgnoreUpdate]</c>, <c>[NotMapped]</c>, and <c>[ReadOnly(true)]</c>.
        /// </summary>
        /// <typeparam name="T">The entity type to update.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToUpdate">The entity instance with updated values.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="entityToDelete">The entity instance whose key values will be used.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="id">The key value. For composite keys, supply an object with matching property names.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The number of rows affected.</returns>
        /// <exception cref="ArgumentException">Thrown when no key property can be located.</exception>
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
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="whereConditions">Anonymous object of filter properties/values.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The number of rows affected.</returns>
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
        /// Deletes multiple rows using a raw SQL <c>WHERE</c> fragment with optional parameters.
        /// </summary>
        /// <typeparam name="T">The entity type to delete.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The number of rows affected.</returns>
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
        /// Returns the number of rows that match an optional <c>WHERE</c> clause.
        /// </summary>
        /// <typeparam name="T">The entity type to count.</typeparam>
        /// <param name="connection">The database connection.</param>
        /// <param name="conditions">A SQL <c>WHERE</c> fragment (with or without the <c>WHERE</c> keyword).</param>
        /// <param name="parameters">Optional parameters object.</param>
        /// <param name="transaction">Optional transaction to enlist commands in.</param>
        /// <param name="commandTimeout">Optional command timeout in seconds.</param>
        /// <returns>The number of matching rows.</returns>
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

        // util: reflect anonymous object props
        private static IEnumerable<PropertyInfo>? GetAllProperties(object? obj)
        {
            return obj?.GetType().GetProperties();
        }
    }
}