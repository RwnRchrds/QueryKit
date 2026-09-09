using QueryKit.Attributes;
using QueryKit.Interfaces;
using System;
using System.Linq;
using System.Reflection;

namespace QueryKit.Metadata;

/// <summary>
/// Default implementation of <see cref="ITableNameResolver"/> that inspects
/// a <c>[Table]</c> attribute (if present) and otherwise returns <c>Type.Name</c>.
/// </summary>
public sealed class TableNameResolver : ITableNameResolver
{
    /// <inheritdoc />
    public string ResolveTableName(Type type)
    {
        // 1) Prefer QueryKit's TableAttribute if present
        var qk = type.GetCustomAttributes(true).OfType<TableAttribute>().FirstOrDefault();
        if (qk is not null && !string.IsNullOrWhiteSpace(qk.Name))
            return string.IsNullOrWhiteSpace(qk.Schema) ? qk.Name : $"{qk.Schema}.{qk.Name}";

        // 2) Fallback: accept other TableAttribute types (e.g. DataAnnotations)
        var anyTableAttr = type.GetCustomAttributes(true)
            .FirstOrDefault(a => a.GetType().Name == "TableAttribute");

        if (anyTableAttr is not null)
        {
            var at = anyTableAttr.GetType();
            var name = at.GetProperty("Name", BindingFlags.Public | BindingFlags.Instance)?.GetValue(anyTableAttr) as string;
            var schema = at.GetProperty("Schema", BindingFlags.Public | BindingFlags.Instance)?.GetValue(anyTableAttr) as string;

            if (!string.IsNullOrWhiteSpace(name))
                return string.IsNullOrWhiteSpace(schema) ? name : $"{schema}.{name}";
        }

        return type.Name;
    }
}