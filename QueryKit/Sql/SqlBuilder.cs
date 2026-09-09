using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using QueryKit.Attributes;
using QueryKit.Extensions;

namespace QueryKit.Sql
{
    internal sealed class SqlBuilder
    {
        private readonly SqlConvention _conv;

        internal SqlBuilder(SqlConvention conv)
        {
            _conv = conv;
        }

        internal void BuildSelect(StringBuilder sb, IEnumerable<PropertyInfo> props)
        {
            var addedAny = false;

            foreach (var p in props)
            {
                if (Attribute.IsDefined(p, typeof(IgnoreSelectAttribute), inherit: true)) continue;
                if (IsNotMapped(p)) continue;

                var rawCol = _conv.GetColumnName(p);
                if (string.IsNullOrWhiteSpace(rawCol))
                    throw new ArgumentException($"Property '{p.DeclaringType?.Name}.{p.Name}' is not mapped to a column.");

                if (addedAny) sb.Append(", ");

                // Always select the encapsulated DB column name
                sb.Append(_conv.Encapsulate(rawCol));

                // Alias back only when the resolved column name differs from the CLR name
                if (!rawCol.Equals(p.Name, StringComparison.OrdinalIgnoreCase))
                {
                    sb.Append(" AS ").Append(_conv.Encapsulate(p.Name));
                }

                addedAny = true;
            }

            if (!addedAny)
                throw new ArgumentException("No selectable columns were found. Check IgnoreSelect/NotMapped/IgnoreCrud attributes.");
        }

        internal void BuildWhere<TEntity>(StringBuilder sb, IEnumerable<PropertyInfo> props, object? where = null)
        {
            var arr = props.ToArray();
            var entityProps = typeof(TEntity).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            for (var i = 0; i < arr.Length; i++)
            {
                var useIsNull = false;
                var incomingProp = arr[i];

                // Map incoming prop (often from anon object) to entity prop
                var entityProp = entityProps.FirstOrDefault(sp =>
                    sp.Name.Equals(incomingProp.Name, StringComparison.OrdinalIgnoreCase));

                if (entityProp == null)
                    throw new ArgumentException(
                        $"Property '{incomingProp.Name}' in the where-condition object does not exist on {typeof(TEntity).Name}.");

                if (where != null && incomingProp.CanRead)
                {
                    var val = incomingProp.GetValue(where, null);
                    if (val == null || val == DBNull.Value) useIsNull = true;
                }

                var rawCol = _conv.GetColumnName(entityProp);
                if (string.IsNullOrWhiteSpace(rawCol))
                    throw new ArgumentException($"Property '{typeof(TEntity).Name}.{entityProp.Name}' is not mapped to a column.");

                var colSql = _conv.Encapsulate(rawCol);

                sb.AppendFormat(useIsNull ? "{0} IS NULL" : "{0} = @{1}", colSql, entityProp.Name);

                if (i < arr.Length - 1) sb.Append(" AND ");
            }
        }

        internal void BuildInsertParameters<T>(StringBuilder sb)
        {
            var addedAny = false;

            foreach (var p in GetInsertableProperties<T>())
            {
                var rawCol = _conv.GetColumnName(p);
                if (string.IsNullOrWhiteSpace(rawCol))
                    throw new ArgumentException($"Property '{typeof(T).Name}.{p.Name}' is not mapped to a column.");

                if (addedAny) sb.Append(", ");
                sb.Append(_conv.Encapsulate(rawCol));
                addedAny = true;
            }

            if (!addedAny)
                throw new ArgumentException($"No insertable columns were found for {typeof(T).Name}.");
        }

        internal void BuildInsertValues<T>(StringBuilder sb)
        {
            var addedAny = false;

            foreach (var p in GetInsertableProperties<T>())
            {
                if (addedAny) sb.Append(", ");
                sb.Append('@').Append(p.Name);
                addedAny = true;
            }
        }

        /// <summary>
        /// One row's placeholders for a multi-row VALUES clause, suffixed with the row's position so
        /// each row in the statement carries its own parameters: (@Id_0, @Name_0), (@Id_1, @Name_1).
        /// </summary>
        internal void BuildInsertValuesForRow<T>(StringBuilder sb, int rowIndex)
        {
            var addedAny = false;

            foreach (var p in GetInsertableProperties<T>())
            {
                if (addedAny) sb.Append(", ");
                sb.Append('@').Append(p.Name).Append('_').Append(rowIndex);
                addedAny = true;
            }
        }

        /// <summary>The properties a batch insert writes, in the order its VALUES clause uses.</summary>
        internal static IReadOnlyList<PropertyInfo> GetInsertablePropertyList<T>() =>
            GetInsertableProperties<T>().ToArray();

        /// <summary>
        /// The columns an upsert overwrites when the key is already there: everything it would have
        /// inserted, less the key itself — that is what identified the row — and less anything
        /// marked read-only or excluded from updates.
        /// </summary>
        internal static IReadOnlyList<PropertyInfo> GetUpsertUpdatePropertyList<T>()
        {
            var keys = SqlConvention.GetIdProperties(typeof(T)).Select(p => p.Name).ToArray();

            return GetInsertableProperties<T>()
                .Where(p => !keys.Contains(p.Name, StringComparer.OrdinalIgnoreCase))
                .Where(p => !Attribute.IsDefined(p, typeof(ReadOnlyAttribute), inherit: true))
                .Where(p => !Attribute.IsDefined(p, typeof(IgnoreUpdateAttribute), inherit: true))
                .ToArray();
        }

        internal void BuildUpdateSet<T>(T entity, StringBuilder sb)
        {
            var props = GetUpdateableProperties(entity);
            var addedAny = false;

            foreach (var p in props)
            {
                var rawCol = _conv.GetColumnName(p);
                if (string.IsNullOrWhiteSpace(rawCol))
                    throw new ArgumentException($"Property '{typeof(T).Name}.{p.Name}' is not mapped to a column.");

                if (addedAny) sb.Append(", ");
                sb.AppendFormat("{0} = @{1}", _conv.Encapsulate(rawCol), p.Name);
                addedAny = true;
            }
        }

        internal static IEnumerable<PropertyInfo> GetScaffoldableProperties<T>()
        {
            var props = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);

            return props
                // global "QueryKit don't touch this"
                .Where(p => !Attribute.IsDefined(p, typeof(IgnoreCrudAttribute), inherit: true))
                // not a DB column (also accepts foreign NotMappedAttribute, e.g. DataAnnotations.Schema)
                .Where(p => !IsNotMapped(p))
                // default: simple types only, override: [Scaffold]
                .Where(p => p.PropertyType.IsSimpleType() ||
                            Attribute.IsDefined(p, typeof(ScaffoldAttribute), inherit: true));
        }

        private static bool IsNotMapped(PropertyInfo p)
        {
            if (Attribute.IsDefined(p, typeof(NotMappedAttribute), inherit: true)) return true;
            var attrs = p.GetCustomAttributes(true);
            for (int i = 0; i < attrs.Length; i++)
                if (attrs[i].GetType().Name == "NotMappedAttribute") return true;
            return false;
        }

        // Properties that should appear in INSERT statements.
        // Auto-identity skip applies only to a *single* int/long primary key (or conventional "Id"
        // when no [Key] attributes exist). Composite-key parts are always included so callers can
        // supply their values explicitly.
        internal static IEnumerable<PropertyInfo> GetInsertableProperties<T>()
        {
            var props = GetScaffoldableProperties<T>().ToArray();
            var keyCount = props.Count(p => Attribute.IsDefined(p, typeof(KeyAttribute), inherit: true));
            return props.Where(p => IsInsertable(p, keyCount));
        }

        private static bool IsInsertable(PropertyInfo p, int keyCount)
        {
            if (Attribute.IsDefined(p, typeof(IgnoreInsertAttribute), inherit: true))
                return false;

            var hasKeyAttr = Attribute.IsDefined(p, typeof(KeyAttribute), inherit: true);
            var isConventionalId =
                keyCount == 0 && p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase);

            // Non-key columns: always include.
            if (!hasKeyAttr && !isConventionalId)
                return true;

            // Key columns: caller-supplied values are always included.
            if (Attribute.IsDefined(p, typeof(RequiredAttribute), inherit: true))
                return true;
            if (p.PropertyType == typeof(Guid) || p.PropertyType == typeof(string))
                return true;

            // Composite keys: include every part — none are auto-identity.
            if (keyCount > 1)
                return true;

            // Single int/long key (or conventional "Id"): skip — assumed auto-identity.
            return false;
        }

        internal static IEnumerable<PropertyInfo> GetUpdateableProperties<T>(T entity)
        {
            return GetScaffoldableProperties<T>()
                .Where(p => !p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.Name.Equals("Version", StringComparison.OrdinalIgnoreCase))
                .Where(p => !Attribute.IsDefined(p, typeof(KeyAttribute), inherit: true))
                .Where(p => !Attribute.IsDefined(p, typeof(VersionAttribute), inherit: true))
                .Where(p => !Attribute.IsDefined(p, typeof(ReadOnlyAttribute), inherit: true))
                .Where(p => !Attribute.IsDefined(p, typeof(IgnoreUpdateAttribute), inherit: true));
        }
    }
}