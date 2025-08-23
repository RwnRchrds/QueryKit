using System.Reflection;
using QueryKit.Interfaces;

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
            var attrs = propertyInfo.GetCustomAttributes(true);
            foreach (var a in attrs)
            {
                if (a.GetType().Name == "ColumnAttribute")
                {
                    var prop = a.GetType().GetProperty("Name");
                    if (prop?.GetValue(a) is string s && !string.IsNullOrWhiteSpace(s)) return s;
                }
            }
            return propertyInfo.Name;
        }
    }
}