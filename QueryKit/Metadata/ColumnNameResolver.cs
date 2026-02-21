using QueryKit.Attributes;
using QueryKit.Interfaces;
using System.Linq;
using System.Reflection;

namespace QueryKit.Metadata
{
    /// <summary>
    /// Default implementation of <see cref="IColumnNameResolver"/> that inspects
    /// <see cref="Attributes.ColumnAttribute"/> (if present) and otherwise returns <see cref="MemberInfo.Name"/>.
    /// </summary>
    public sealed class ColumnNameResolver : IColumnNameResolver
    {
        /// <inheritdoc />
        public string ResolveColumnName(PropertyInfo propertyInfo)
        {
            var colAttr = propertyInfo.GetCustomAttributes(true).OfType<ColumnAttribute>().FirstOrDefault();
            if (colAttr != null && !string.IsNullOrWhiteSpace(colAttr.Name))
                return colAttr.Name;

            return propertyInfo.Name;
        }
    }
}