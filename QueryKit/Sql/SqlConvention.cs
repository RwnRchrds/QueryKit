using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using QueryKit.Attributes;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Interfaces;
using QueryKit.Metadata;

namespace QueryKit.Sql;

/// <summary>
/// Convention class for resolving table and column names, encapsulation, and other SQL-related conventions.
/// </summary>
public sealed class SqlConvention
{
    private const double SqlServerTickMs = 3.3333333333333335;

    private static readonly DateTime BaseDateUtc = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    private readonly DialectConfig _dialect;
    private readonly ITableNameResolver _table;
    private readonly IColumnNameResolver _column;

    private readonly ConcurrentDictionary<Type, string> _tableNames = new();
    private readonly ConcurrentDictionary<string, string?> _columnNames = new();

    // Cache resolved version properties (reflection is not free)
    private static readonly ConcurrentDictionary<Type, PropertyInfo> _versionProps = new();

    /// <summary>
    /// Instantiates a new instance of the SqlConvention class using default dialect and resolvers.
    /// </summary>
    public SqlConvention()
    {
        _dialect = ConnectionExtensions.Config;
        _table = new TableNameResolver();
        _column = new ColumnNameResolver();
    }

    /// <summary>
    /// Instantiates a new instance of the SqlConvention class with specified dialect and resolvers.
    /// </summary>
    public SqlConvention(DialectConfig dialect, ITableNameResolver tableResolver, IColumnNameResolver columnResolver)
    {
        _dialect = dialect;
        _table = tableResolver;
        _column = columnResolver;
    }

    /// <summary>
    /// Encapsulates an identifier (table or column name) according to the SQL dialect's rules.
    /// </summary>
    public string Encapsulate(string identifier) => string.Format(_dialect.Encapsulation, identifier);

    /// <summary>
    /// Retrieves the table name for a given type, using caching for performance.
    /// </summary>
    public string GetTableName(Type type) =>
        _tableNames.GetOrAdd(type, t => _table.ResolveTableName(t));

    /// <summary>
    /// Retrieves the table name for a given entity instance.
    /// </summary>
    public string GetTableName(object entity) => GetTableName(entity.GetType());

    /// <summary>
    /// Retrieves the column name for a given property, using caching for performance.
    /// </summary>
    public string? GetColumnName(PropertyInfo pi)
    {
        var key = $"{pi.DeclaringType}.{pi.Name}";
        return _columnNames.GetOrAdd(key, _ => _column.ResolveColumnName(pi));
    }

    /// <summary>
    /// Returns a new sequential GUID based on the current timestamp.
    /// </summary>
    public static Guid SequentialGuid()
    {
        var guidArray = Guid.NewGuid().ToByteArray();

        var now = DateTime.UtcNow;
        var days = (now - BaseDateUtc).Days; // fits in 2 bytes until year 2079
        var msecs = (int)(now.TimeOfDay.TotalMilliseconds / SqlServerTickMs);

        var daysArray = BitConverter.GetBytes((short)days);   // 2 bytes
        var msecsArray = BitConverter.GetBytes(msecs);        // 4 bytes

        // SQL Server’s GUID ordering expects these reversed here.
        Array.Reverse(daysArray);
        Array.Reverse(msecsArray);

        // Write into the LAST 6 bytes of the GUID
        // [.... .... .... .... .... dd mm mm mm mm]
        Array.Copy(daysArray, 0, guidArray, guidArray.Length - 6, 2);
        Array.Copy(msecsArray, 0, guidArray, guidArray.Length - 4, 4);

        return new Guid(guidArray);
    }

    /// <summary>
    /// Returns true if the property is marked as editable via the EditableAttribute.
    /// </summary>
    public static bool IsEditable(PropertyInfo pi)
    {
        var attrs = pi.GetCustomAttributes(false);
        if (attrs.Length > 0)
        {
            dynamic? write = attrs.FirstOrDefault(x => x is EditableAttribute);
            if (write != null) return write.AllowEdit;
        }
        return false;
    }

    /// <summary>
    /// Returns true if the property is marked as read-only via the ReadOnlyAttribute.
    /// </summary>
    public static bool IsReadOnly(PropertyInfo pi)
    {
        var attrs = pi.GetCustomAttributes(false);
        if (attrs.Length > 0)
        {
            dynamic? ro = attrs.FirstOrDefault(x => x is ReadOnlyAttribute);
            if (ro != null) return ro.IsReadOnly;
        }
        return false;
    }

    /// <summary>
    /// Returns the properties of a type that are considered identifier properties.
    /// Prefer properties marked with [Key]; otherwise falls back to a property named "Id".
    /// </summary>
    public static PropertyInfo[] GetIdProperties(Type type)
    {
        var keyed = type.GetProperties()
            .Where(p => p.GetCustomAttributes(true).Any(a => a is KeyAttribute))
            .ToList();

        return (keyed.Any()
            ? keyed
            : type.GetProperties().Where(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)))
            .ToArray();
    }

    /// <summary>
    /// Returns the property of a type that is used for optimistic concurrency control,
    /// either marked with [Version] or named "Version".
    /// The version property must be of type long (non-nullable).
    /// </summary>
    public static PropertyInfo GetVersionProperty(Type type)
        => _versionProps.GetOrAdd(type, ResolveVersionProperty);

    private static PropertyInfo ResolveVersionProperty(Type type)
    {
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        var marked = props
            .Where(p => p.GetCustomAttributes(true).Any(a => a is VersionAttribute))
            .ToArray();

        if (marked.Length > 1)
        {
            throw new ArgumentException(
                $"{type.Name} has multiple properties marked with [{nameof(VersionAttribute)}]. Only one is allowed.");
        }

        var prop = marked.Length == 1
            ? marked[0]
            : props.FirstOrDefault(p => p.Name.Equals("Version", StringComparison.OrdinalIgnoreCase));

        if (prop == null)
        {
            throw new ArgumentException(
                $"{type.Name} must have a public long Version property, or a property marked with [{nameof(VersionAttribute)}].");
        }

        // long only (non-nullable)
        if (prop.PropertyType != typeof(long))
        {
            throw new ArgumentException(
                $"{type.Name}.{prop.Name} must be of type long (non-nullable) to use optimistic concurrency.");
        }

        return prop;
    }

    /// <summary>
    /// Returns the identifier properties of an entity instance.
    /// </summary>
    public static PropertyInfo[] GetIdProperties(object entity) => GetIdProperties(entity.GetType());
}
