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

    private readonly ConcurrentDictionary<Type,string> _tableNames = new();
    private readonly ConcurrentDictionary<string, string?> _columnNames = new();

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
    /// <param name="dialect"></param>
    /// <param name="tableResolver"></param>
    /// <param name="columnResolver"></param>
    public SqlConvention(DialectConfig dialect, ITableNameResolver tableResolver, IColumnNameResolver columnResolver)
    { _dialect = dialect; _table = tableResolver; _column = columnResolver; }

    /// <summary>
    /// Encapsulates an identifier (table or column name) according to the SQL dialect's rules.
    /// </summary>
    /// <param name="identifier"></param>
    /// <returns></returns>
    public string Encapsulate(string identifier) => string.Format(_dialect.Encapsulation, identifier);

    /// <summary>
    /// Retrieves the table name for a given type, using caching for performance.
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public string GetTableName(Type type) =>
        _tableNames.GetOrAdd(type, t => _table.ResolveTableName(t));

    /// <summary>
    /// Retrieves the table name for a given entity instance.
    /// </summary>
    /// <param name="entity"></param>
    /// <returns></returns>
    public string GetTableName(object entity) => GetTableName(entity.GetType());

    /// <summary>
    /// Retrieves the column name for a given property, using caching for performance.
    /// </summary>
    /// <param name="pi"></param>
    /// <returns></returns>
    public string GetColumnName(PropertyInfo pi)
    {
        var key = $"{pi.DeclaringType}.{pi.Name}";
        return _columnNames.GetOrAdd(key, _ => _column.ResolveColumnName(pi));
    }

    /// <summary>
    /// Returns a new sequential GUID based on the current timestamp.
    /// </summary>
    /// <returns></returns>
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
    /// <param name="pi"></param>
    /// <returns></returns>
    public static bool IsEditable(PropertyInfo pi)
    {
        var attrs = pi.GetCustomAttributes(false);
        if (attrs.Length > 0)
        {
            dynamic? write = attrs.FirstOrDefault(x => x.GetType().Name == nameof(EditableAttribute));
            if (write != null) return write.AllowEdit;
        }
        return false;
    }

    /// <summary>
    /// Returns true if the property is marked as read-only via the ReadOnlyAttribute.
    /// </summary>
    /// <param name="pi"></param>
    /// <returns></returns>
    public static bool IsReadOnly(PropertyInfo pi)
    {
        var attrs = pi.GetCustomAttributes(false);
        if (attrs.Length > 0)
        {
            dynamic? ro = attrs.FirstOrDefault(x => x.GetType().Name == nameof(ReadOnlyAttribute));
            if (ro != null) return ro.IsReadOnly;
        }
        return false;
    }

    /// <summary>
    /// Returns the properties of a type that are considered identifier properties.
    /// </summary>
    /// <param name="type"></param>
    /// <returns></returns>
    public static PropertyInfo[] GetIdProperties(Type type)
    {
        var keyed = type.GetProperties().Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == nameof(KeyAttribute))).ToList();
        return (keyed.Any() ? keyed : type.GetProperties().Where(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))).ToArray();
    }

    /// <summary>
    /// Returns the identifier properties of an entity instance.
    /// </summary>
    /// <param name="entity"></param>
    /// <returns></returns>
    public static PropertyInfo[] GetIdProperties(object entity) => GetIdProperties(entity.GetType());
}