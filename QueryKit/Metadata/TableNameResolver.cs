using System;
using QueryKit.Interfaces;

namespace QueryKit.Metadata
{
    /// <summary>
    /// Default implementation of <see cref="ITableNameResolver"/> that inspects
    /// a <c>[Table]</c> attribute (if present) and otherwise returns <see cref="Type.Name"/>.
    /// </summary>
    public sealed class TableNameResolver : ITableNameResolver
    {
        /// <inheritdoc />
        public string ResolveTableName(Type type)
        {
            // Reads [Table("Name")] if present; else type.Name
            var tableAttr = type.GetCustomAttributes(true);
            var name = type.Name;
            foreach (var a in tableAttr)
            {
                if (a.GetType().Name == "TableAttribute")
                {
                    var prop = a.GetType().GetProperty("Name");
                    if (prop?.GetValue(a) is string s && !string.IsNullOrWhiteSpace(s)) { name = s; break; }
                }
            }
            return name;
        }
    }
}