using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Sql;

namespace QueryKit.Extensions
{
    /// <summary>
    /// Insert-or-update in one statement, spelled the way each dialect spells it.
    /// </summary>
    public static class UpsertExtensions
    {
        /// <summary>
        /// Asynchronously inserts entities, updating any whose key is already present, and returns
        /// the number of rows the statement reported. Columns are derived exactly as for an insert,
        /// so <c>[Table]</c>, <c>[Column]</c> and <c>[IgnoreCrud]</c> behave identically; the
        /// columns overwritten on a match are those less the key, <c>[ReadOnly]</c> and
        /// <c>[IgnoreUpdate]</c>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// The match is on the entity's key, so the table needs a unique index or primary key over
        /// it — which is what a key means, but it is worth saying, because without one every dialect
        /// here inserts a duplicate instead of updating and reports success.
        /// </para>
        /// <para>
        /// <paramref name="batchSize"/> sets the rows per statement, defaulting to a count that
        /// keeps a batch inside the parameter limit providers impose (SQL Server allows 2100).
        /// </para>
        /// </remarks>
        /// <exception cref="NotSupportedException">
        /// The entity has an identity key. A generated key cannot identify an existing row, so there
        /// is nothing for the update half to match on.
        /// </exception>
        public static async Task<int> UpsertAsync<T>(this IDbConnection connection,
            IEnumerable<T> entities, IDbTransaction? transaction = null,
            int? commandTimeout = null, int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (entities is null) throw new ArgumentNullException(nameof(entities));

            var rows = entities as IList<T> ?? entities.ToList();
            if (rows.Count == 0) return 0;

            var conv = ConnectionExtensions.NewConvention();
            var builder = ConnectionExtensions.NewBuilder(conv);

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);
            var keyProps = SqlConvention.GetIdProperties(type);

            if (keyProps == null || keyProps.Length == 0)
                throw new ArgumentException("UpsertAsync<T> requires an entity with a [Key] or Id property.");

            foreach (var key in keyProps)
            {
                if (key.PropertyType != typeof(Guid) && key.PropertyType != typeof(string) &&
                    keyProps.Length == 1)
                {
                    throw new NotSupportedException(
                        $"UpsertAsync<T> cannot match {type.Name} on an identity key: the database " +
                        "generates it, so it cannot identify a row that already exists. Use a Guid " +
                        "or string key, or a composite key of supplied values.");
                }
            }

            var columns = SqlBuilder.GetInsertablePropertyList<T>();
            if (columns.Count == 0)
                throw new ArgumentException($"No insertable columns were found for {type.Name}.");

            var updates = SqlBuilder.GetUpsertUpdatePropertyList<T>();

            var perBatch = batchSize ?? Math.Max(1, 2000 / columns.Count);
            var style = ConnectionExtensions.Config.UpsertStyle;
            var affected = 0;

            for (var offset = 0; offset < rows.Count; offset += perBatch)
            {
                var take = Math.Min(perBatch, rows.Count - offset);
                var parameters = new DynamicParameters();

                // Fill empty Guid keys before the SQL is built, so the values bound below and the
                // entity the caller keeps hold of agree on what the key is.
                for (var i = 0; i < take; i++)
                {
                    var entity = rows[offset + i];
                    if (entity is null)
                        throw new ArgumentException("UpsertAsync<T> was given a null entity.");

                    foreach (var key in keyProps)
                    {
                        if (key.PropertyType != typeof(Guid)) continue;
                        var current = (Guid)(key.GetValue(entity, null) ?? Guid.Empty);
                        if (current == Guid.Empty)
                            key.SetValue(entity, SqlConvention.SequentialGuid(), null);
                    }

                    foreach (var col in columns)
                        parameters.Add("@" + col.Name + "_" + i, col.GetValue(entity, null));
                }

                var sql = style == UpsertStyle.Merge
                    ? BuildMerge<T>(conv, table, columns, keyProps, updates, take)
                    : BuildInsertOnDuplicate<T>(conv, table, columns, keyProps, updates, take, style);

                ConnectionExtensions.Log(() => $"UpsertAsync<{type.Name}>: {sql}");

                affected += await connection.ExecuteAsync(new CommandDefinition(sql, parameters,
                    transaction, commandTimeout, cancellationToken: cancellationToken));
            }

            return affected;
        }

        // Deliberately no UpsertAsync<T>(this IDbConnection, T entity) overload. An array argument
        // binds to it exactly, as T = Person[], in preference to the IEnumerable<T> overload that
        // needs a conversion — so callers passing a collection would silently upsert the collection
        // as a single entity. One entity is written as UpsertAsync(new[] { entity }).

        private static string ColumnList(SqlConvention conv, IReadOnlyList<PropertyInfo> props)
            => string.Join(", ", props.Select(p => conv.GetColumnNameEncapsulated(p)));

        private static string RowValues(IReadOnlyList<PropertyInfo> props, int row)
            => string.Join(", ", props.Select(p => "@" + p.Name + "_" + row));

        /// <summary>
        /// <c>INSERT ... ON CONFLICT (key) DO UPDATE</c> for PostgreSQL and SQLite, and
        /// <c>ON DUPLICATE KEY UPDATE</c> for MySQL, which names the excluded row differently and
        /// takes no conflict target because it uses whichever unique index was violated.
        /// </summary>
        private static string BuildInsertOnDuplicate<T>(SqlConvention conv, string table,
            IReadOnlyList<PropertyInfo> columns, PropertyInfo[] keyProps,
            IReadOnlyList<PropertyInfo> updates, int take, UpsertStyle style)
        {
            var sb = new StringBuilder();
            sb.AppendFormat("insert into {0} ({1}) values ", table, ColumnList(conv, columns));

            for (var row = 0; row < take; row++)
            {
                if (row > 0) sb.Append(", ");
                sb.AppendFormat("({0})", RowValues(columns, row));
            }

            if (updates.Count == 0)
            {
                // Nothing to overwrite: the row is entirely key. Saying so is better than emitting
                // an empty SET, which is a syntax error everywhere.
                sb.Append(style == UpsertStyle.OnConflict
                    ? string.Format(" on conflict ({0}) do nothing", ColumnList(conv, keyProps))
                    : " on duplicate key update " +
                      conv.GetColumnNameEncapsulated(keyProps[0]) + " = " +
                      conv.GetColumnNameEncapsulated(keyProps[0]));
                return sb.ToString();
            }

            if (style == UpsertStyle.OnConflict)
            {
                sb.AppendFormat(" on conflict ({0}) do update set ", ColumnList(conv, keyProps));
                sb.Append(string.Join(", ", updates.Select(p =>
                {
                    var col = conv.GetColumnNameEncapsulated(p);
                    return col + " = excluded." + col;
                })));
            }
            else
            {
                sb.Append(" on duplicate key update ");
                sb.Append(string.Join(", ", updates.Select(p =>
                {
                    var col = conv.GetColumnNameEncapsulated(p);
                    return col + " = values(" + col + ")";
                })));
            }

            return sb.ToString();
        }

        /// <summary>
        /// <c>MERGE</c> for SQL Server, Oracle and Db2. Oracle has no multi-row VALUES, so its source
        /// is a union of selects from dual rather than a values list.
        /// </summary>
        private static string BuildMerge<T>(SqlConvention conv, string table,
            IReadOnlyList<PropertyInfo> columns, PropertyInfo[] keyProps,
            IReadOnlyList<PropertyInfo> updates, int take)
        {
            var cols = ColumnList(conv, columns);
            var sb = new StringBuilder();

            sb.AppendFormat("merge into {0} as target using (", table);

            if (ConnectionExtensions.Config.SupportsMultiRowValues)
            {
                sb.Append("values ");
                for (var row = 0; row < take; row++)
                {
                    if (row > 0) sb.Append(", ");
                    sb.AppendFormat("({0})", RowValues(columns, row));
                }
                sb.AppendFormat(") as source ({0})", cols);
            }
            else
            {
                for (var row = 0; row < take; row++)
                {
                    if (row > 0) sb.Append(" union all ");
                    sb.Append("select ");
                    sb.Append(string.Join(", ", columns.Select(p =>
                        "@" + p.Name + "_" + row + " as " + conv.GetColumnNameEncapsulated(p))));
                    sb.Append(" from dual");
                }
                sb.Append(") source");
            }

            sb.Append(" on ");
            sb.Append(string.Join(" and ", keyProps.Select(p =>
            {
                var col = conv.GetColumnNameEncapsulated(p);
                return "target." + col + " = source." + col;
            })));

            if (updates.Count > 0)
            {
                sb.Append(" when matched then update set ");
                sb.Append(string.Join(", ", updates.Select(p =>
                {
                    var col = conv.GetColumnNameEncapsulated(p);
                    return col + " = source." + col;
                })));
            }

            sb.AppendFormat(" when not matched then insert ({0}) values ({1})", cols,
                string.Join(", ", columns.Select(p => "source." + conv.GetColumnNameEncapsulated(p))));

            // SQL Server requires the terminator; the others tolerate it.
            sb.Append(";");
            return sb.ToString();
        }
    }
}
