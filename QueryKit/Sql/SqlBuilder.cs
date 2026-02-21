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
            var list = props.ToList();
            var addedAny = false;

            for (var i = 0; i < list.Count; i++)
            {
                var p = list[i];

                if (p.GetCustomAttributes(true).Any(a =>
                        a.GetType().Name == nameof(IgnoreSelectAttribute) ||
                        a.GetType().Name == nameof(NotMappedAttribute)))
                    continue;

                var rawCol = _conv.GetColumnName(p);
                if (string.IsNullOrWhiteSpace(rawCol))
                    throw new ArgumentException($"Property '{p.DeclaringType?.Name}.{p.Name}' is not mapped to a column.");

                if (addedAny) sb.Append(", ");

                // Always select the encapsulated DB column name
                sb.Append(_conv.Encapsulate(rawCol));

                // Alias back only when [Column] exists AND CLR name differs from raw column name
                var colAttr = p.GetCustomAttributes(true).OfType<ColumnAttribute>().FirstOrDefault();
                if (colAttr != null && !rawCol.Equals(p.Name, StringComparison.OrdinalIgnoreCase))
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
            var props = GetScaffoldableProperties<T>().ToArray();
            var addedAny = false;

            foreach (var p in props)
            {
                // Skip identity int/long keys unless [Required]
                if (p.PropertyType != typeof(Guid) && p.PropertyType != typeof(string)
                                                   && Attribute.IsDefined(p, typeof(KeyAttribute), inherit: true)
                                                   && !Attribute.IsDefined(p, typeof(RequiredAttribute), inherit: true))
                    continue;

                if (Attribute.IsDefined(p, typeof(IgnoreInsertAttribute), inherit: true))
                    continue;

                // Skip conventional Id for non-guid unless [Required]
                if (p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
                    && !Attribute.IsDefined(p, typeof(RequiredAttribute), inherit: true)
                    && p.PropertyType != typeof(Guid))
                    continue;

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
            var props = GetScaffoldableProperties<T>().ToArray();
            var addedAny = false;

            foreach (var p in props)
            {
                if (p.PropertyType != typeof(Guid) && p.PropertyType != typeof(string)
                                                   && Attribute.IsDefined(p, typeof(KeyAttribute), inherit: true)
                                                   && !Attribute.IsDefined(p, typeof(RequiredAttribute), inherit: true))
                    continue;

                if (Attribute.IsDefined(p, typeof(IgnoreInsertAttribute), inherit: true))
                    continue;

                if (p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
                    && !Attribute.IsDefined(p, typeof(RequiredAttribute), inherit: true)
                    && p.PropertyType != typeof(Guid))
                    continue;

                if (addedAny) sb.Append(", ");
                sb.Append('@').Append(p.Name);
                addedAny = true;
            }
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
                // not a DB column
                .Where(p => !Attribute.IsDefined(p, typeof(NotMappedAttribute), inherit: true))
                // default: simple types only, override: [Scaffold]
                .Where(p => p.PropertyType.IsSimpleType() ||
                            Attribute.IsDefined(p, typeof(ScaffoldAttribute), inherit: true));
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