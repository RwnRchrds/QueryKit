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

        internal SqlBuilder(SqlConvention conv) { _conv = conv; }

        internal void BuildSelect(StringBuilder sb, IEnumerable<PropertyInfo> props)
        {
            var list = props.ToList();
            var addedAny = false;
            for (var i = 0; i < list.Count; i++)
            {
                var p = list[i];
                if (p.GetCustomAttributes(true).Any(a => a.GetType().Name == nameof(IgnoreSelectAttribute) || a.GetType().Name == nameof(NotMappedAttribute)))
                    continue;

                if (addedAny) sb.Append(",");
                sb.Append(_conv.GetColumnName(p));

                // If [Column("db_name")] and CLR name differs, alias it back
                if (p.GetCustomAttributes(true).SingleOrDefault(a => a.GetType().Name == nameof(ColumnAttribute)) != null)
                    sb.Append(" as " + _conv.Encapsulate(p.Name));

                addedAny = true;
            }
        }

        internal void BuildWhere<TEntity>(StringBuilder sb, IEnumerable<PropertyInfo> props, object? where = null)
        {
            var arr = props.ToArray();
            for (var i = 0; i < arr.Length; i++)
            {
                var useIsNull = false;
                var propToUse = arr[i];

                // map to the source property to fetch correct [Column] mapping
                var src = typeof(TEntity).GetProperties();
                foreach (var sp in src)
                {
                    if (sp.Name == propToUse.Name)
                    {
                        if (where != null && propToUse.CanRead)
                        {
                            var val = propToUse.GetValue(where, null);
                            if (val == null || val == DBNull.Value) useIsNull = true;
                        }
                        propToUse = sp; break;
                    }
                }

                sb.AppendFormat(useIsNull ? "{0} is null" : "{0} = @{1}", _conv.GetColumnName(propToUse), propToUse.Name);
                if (i < arr.Length - 1) sb.Append(" and ");
            }
        }

        internal void BuildInsertParameters<T>(StringBuilder sb)
        {
            var props = GetScaffoldableProperties<T>().ToArray();
            for (var i = 0; i < props.Length; i++)
            {
                var p = props[i];
                if (p.PropertyType != typeof(Guid) && p.PropertyType != typeof(string)
                    && p.GetCustomAttributes(true).Any(a => a.GetType().Name == nameof(KeyAttribute))
                    && p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(RequiredAttribute))) continue;

                if (p.GetCustomAttributes(true).Any(a =>
                        a.GetType().Name == nameof(IgnoreInsertAttribute) ||
                        a.GetType().Name == nameof(NotMappedAttribute) ||
                        (a.GetType().Name == nameof(ReadOnlyAttribute) && SqlConvention.IsReadOnly(p)))) continue;

                if (p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
                    && p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(RequiredAttribute))
                    && p.PropertyType != typeof(Guid)) continue;

                sb.Append(_conv.GetColumnName(p));
                if (i < props.Length - 1) sb.Append(", ");
            }
            if (sb.ToString().EndsWith(", ")) sb.Length -= 2;
        }

        internal void BuildInsertValues<T>(StringBuilder sb)
        {
            var props = GetScaffoldableProperties<T>().ToArray();
            for (var i = 0; i < props.Length; i++)
            {
                var p = props[i];
                if (p.PropertyType != typeof(Guid) && p.PropertyType != typeof(string)
                    && p.GetCustomAttributes(true).Any(a => a.GetType().Name == nameof(KeyAttribute))
                    && p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(RequiredAttribute))) continue;

                if (p.GetCustomAttributes(true).Any(a =>
                        a.GetType().Name == nameof(IgnoreInsertAttribute) ||
                        a.GetType().Name == nameof(NotMappedAttribute) ||
                        (a.GetType().Name == nameof(ReadOnlyAttribute) && SqlConvention.IsReadOnly(p)))) continue;

                if (p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)
                    && p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(RequiredAttribute))
                    && p.PropertyType != typeof(Guid)) continue;

                sb.Append($"@{p.Name}");
                if (i < props.Length - 1) sb.Append(", ");
            }
            if (sb.ToString().EndsWith(", ")) sb.Length -= 2;
        }

        internal void BuildUpdateSet<T>(T entity, StringBuilder sb)
        {
            var props = GetUpdateableProperties(entity).ToArray();
            for (var i = 0; i < props.Length; i++)
            {
                var p = props[i];
                sb.AppendFormat("{0} = @{1}", _conv.GetColumnName(p), p.Name);
                if (i < props.Length - 1) sb.Append(", ");
            }
        }
        
        internal static IEnumerable<PropertyInfo> GetScaffoldableProperties<T>()
        {
            var props = typeof(T).GetProperties();
            props = props.Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == nameof(EditableAttribute) && !SqlConvention.IsEditable(p)) == false).ToArray();
            return props.Where(p => p.PropertyType.IsSimpleType() || SqlConvention.IsEditable(p));
        }

        internal static IEnumerable<PropertyInfo> GetUpdateableProperties<T>(T entity)
        {
            var props = GetScaffoldableProperties<T>()
                .Where(p => !p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))
                .Where(p => !p.Name.Equals("Version", StringComparison.OrdinalIgnoreCase))
                .Where(p => p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(KeyAttribute)))
                .Where(p => p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(VersionAttribute)))
                .Where(p => p.GetCustomAttributes(true).All(a => !(a.GetType().Name == nameof(ReadOnlyAttribute) && SqlConvention.IsReadOnly(p))))
                .Where(p => p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(IgnoreUpdateAttribute)))
                .Where(p => p.GetCustomAttributes(true).All(a => a.GetType().Name != nameof(NotMappedAttribute)));
            return props;
        }
    }
}