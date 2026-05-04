using QueryKit.Attributes;
using QueryKit.Interfaces;
using System.Linq;
using System.Reflection;

namespace QueryKit.Metadata
{
    /// <summary>
    /// Default implementation of <see cref="IColumnNameResolver"/> that inspects
    /// <see cref="Attributes.ColumnAttribute"/> (if present) and otherwise returns <see cref="MemberInfo.Name"/>.
    /// As a courtesy, also reads any other attribute named <c>ColumnAttribute</c> (e.g. from
    /// <c>System.ComponentModel.DataAnnotations.Schema</c>) via reflection on its <c>Name</c> property.
    /// </summary>
    public sealed class ColumnNameResolver : IColumnNameResolver
    {
        /// <inheritdoc />
        public string ResolveColumnName(PropertyInfo propertyInfo)
        {
            var colAttr = propertyInfo.GetCustomAttributes(true).OfType<ColumnAttribute>().FirstOrDefault();
            if (colAttr != null && !string.IsNullOrWhiteSpace(colAttr.Name))
                return colAttr.Name;

            // Fallback: accept other ColumnAttribute types (e.g. DataAnnotations.Schema)
            var anyCol = propertyInfo.GetCustomAttributes(true)
                .FirstOrDefault(a => a.GetType().Name == "ColumnAttribute");

            if (anyCol != null)
            {
                var name = anyCol.GetType()
                    .GetProperty("Name", BindingFlags.Public | BindingFlags.Instance)
                    ?.GetValue(anyCol) as string;
                if (!string.IsNullOrWhiteSpace(name))
                    return name!;
            }

            return propertyInfo.Name;
        }
    }
}