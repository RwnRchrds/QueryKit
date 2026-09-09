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
    /// Updating many entities without paying a round trip for each one.
    /// </summary>
    public static class BatchUpdateExtensions
    {
        /// <summary>
        /// Asynchronously updates many entities by key, and returns the number of rows affected.
        /// Each entity updates the same columns <see cref="ConnectionExtensionsAsync.UpdateAsync{T}"/>
        /// would: every mapped column except the key, <c>[ReadOnly]</c> and <c>[IgnoreUpdate]</c>.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Rows differ from one another, so unlike an insert there is no single statement that
        /// covers them all portably. This sends one <c>UPDATE</c> per entity but all of them in one
        /// command, so the cost is one round trip per batch rather than one per row — which is the
        /// part that dominates. Oracle has no multi-statement batch, so its batch is wrapped in a
        /// <c>BEGIN … END;</c> block.
        /// </para>
        /// <para>
        /// <paramref name="batchSize"/> sets the rows per command, defaulting to a count that keeps
        /// a batch inside the parameter limit providers impose (SQL Server allows 2100).
        /// </para>
        /// <para>
        /// This does not touch a <c>[Version]</c> column or check one. Use
        /// <see cref="ConnectionExtensionsAsync.UpdateWithVersionAsync{T}"/> per entity where
        /// optimistic concurrency matters: a batch cannot report which row of many lost a race.
        /// </para>
        /// </remarks>
        public static async Task<int> BatchUpdateAsync<T>(this IDbConnection connection,
            IEnumerable<T> entities, IDbTransaction? transaction = null,
            int? commandTimeout = null, int? batchSize = null,
            CancellationToken cancellationToken = default)
        {
            if (entities is null) throw new ArgumentNullException(nameof(entities));

            var rows = entities as IList<T> ?? entities.ToList();
            if (rows.Count == 0) return 0;

            var conv = ConnectionExtensions.NewConvention();

            var type = typeof(T);
            var table = conv.GetTableNameEncapsulated(type);
            var keyProps = SqlConvention.GetIdProperties(type);

            if (keyProps == null || keyProps.Length == 0)
                throw new ArgumentException("BatchUpdateAsync<T> requires an entity with a [Key] or Id property.");

            var updates = SqlBuilder.GetUpsertUpdatePropertyList<T>();
            if (updates.Count == 0)
                throw new ArgumentException(
                    $"{type.Name} has no updateable columns: every mapped property is a key, " +
                    "[ReadOnly] or [IgnoreUpdate].");

            var perStatement = updates.Count + keyProps.Length;
            var perBatch = batchSize ?? Math.Max(1, 2000 / perStatement);
            var isOracle = ConnectionExtensions.Config.Dialect == Dialect.Oracle;
            var affected = 0;

            for (var offset = 0; offset < rows.Count; offset += perBatch)
            {
                var take = Math.Min(perBatch, rows.Count - offset);
                var parameters = new DynamicParameters();
                var sql = new StringBuilder();

                if (isOracle) sql.Append("begin ");

                for (var i = 0; i < take; i++)
                {
                    var entity = rows[offset + i];
                    if (entity is null)
                        throw new ArgumentException("BatchUpdateAsync<T> was given a null entity.");

                    sql.AppendFormat("update {0} set ", table);
                    sql.Append(string.Join(", ", updates.Select(p =>
                        conv.GetColumnNameEncapsulated(p) + " = @" + p.Name + "_" + i)));

                    sql.Append(" where ");
                    sql.Append(string.Join(" and ", keyProps.Select(p =>
                        conv.GetColumnNameEncapsulated(p) + " = @" + p.Name + "_" + i)));

                    sql.Append("; ");

                    foreach (var p in updates.Concat(keyProps))
                        parameters.Add("@" + p.Name + "_" + i, p.GetValue(entity, null));
                }

                if (isOracle) sql.Append("end;");

                var batchSql = sql.ToString();
                ConnectionExtensions.Log(() => $"BatchUpdateAsync<{type.Name}> ({take} rows): {batchSql}");

                affected += await connection.ExecuteAsync(new CommandDefinition(batchSql, parameters,
                    transaction, commandTimeout, cancellationToken: cancellationToken));
            }

            return affected;
        }
    }
}
