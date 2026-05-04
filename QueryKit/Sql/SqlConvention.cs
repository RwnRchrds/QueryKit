using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using System.Threading;
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

    // BUG #5 FIX: Year-2079 overflow guard. BaseDateUtc + (short.MaxValue days) ≈ 2079-09-18.
    // After that date, the sequential portion wraps. We detect this at generation time and throw
    // rather than silently producing non-sequential GUIDs.
    private static readonly DateTime BaseDateUtc = new(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime MaxSequentialGuidDate = BaseDateUtc.AddDays(ushort.MaxValue); // ~2079-09-18

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
    /// Encapsulates a database identifier, such as a table or column name, to ensure it is properly delimited for use
    /// in SQL statements.
    /// </summary>
    /// <remarks>Use this method to safely format identifiers for SQL queries, preventing issues with reserved
    /// keywords or special characters. Each part of a multipart identifier is individually encapsulated.</remarks>
    /// <param name="identifier">The identifier to encapsulate. This can be a simple name or a multipart identifier separated by periods (e.g.,
    /// schema.table or table.column). Cannot be null or whitespace.</param>
    /// <returns>A string containing the encapsulated identifier, with each part properly delimited for SQL usage.</returns>
    /// <exception cref="ArgumentException">Thrown if the identifier is null or consists only of whitespace.</exception>
    public string Encapsulate(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
            throw new ArgumentException("Identifier cannot be null or whitespace.", nameof(identifier));

        // Split schema/table or table/column etc.
        var parts = identifier.Split(new [] { '.' }, StringSplitOptions.RemoveEmptyEntries);

        if (parts.Length == 0)
        {
            throw new ArgumentException("Identifier is not valid.", nameof(identifier));
        }

        return string.Join(".", parts.Select(p => EncapsulateToken(p.Trim())));
    }

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
        var key = $"{pi.Module.ModuleVersionId}:{pi.MetadataToken}";
        return _columnNames.GetOrAdd(key, _ => _column.ResolveColumnName(pi));
    }

    /// <summary>
    /// Returns the column name for the specified property, encapsulated according to the database provider's
    /// requirements.
    /// </summary>
    /// <remarks>Use this method when you need the column name formatted for use in SQL statements, such as
    /// when quoting or delimiting is required by the database provider.</remarks>
    /// <param name="pi">The property for which to retrieve the encapsulated column name. Must be mapped to a database column.</param>
    /// <returns>A string containing the encapsulated column name for the specified property.</returns>
    /// <exception cref="ArgumentException">Thrown if the specified property is not mapped to a database column.</exception>
    public string GetColumnNameEncapsulated(PropertyInfo pi)
    {
        var raw = GetColumnName(pi);
        if (string.IsNullOrWhiteSpace(raw))
            throw new ArgumentException($"Property '{pi.DeclaringType?.Name}.{pi.Name}' is not mapped to a column.");

        return Encapsulate(raw);
    }

    /// <summary>
    /// Returns the table name for the specified type, encapsulated according to the database's identifier rules.
    /// </summary>
    /// <remarks>The encapsulation format depends on the database provider and may include quoting or
    /// delimiting characters to ensure the table name is valid in SQL statements.</remarks>
    /// <param name="type">The type whose associated table name will be retrieved and encapsulated. Must be mapped to a valid table name.</param>
    /// <returns>A string containing the encapsulated table name for the specified type.</returns>
    /// <exception cref="ArgumentException">Thrown if the specified type is not mapped to a table name.</exception>
    public string GetTableNameEncapsulated(Type type)
    {
        var raw = GetTableName(type);
        if (string.IsNullOrWhiteSpace(raw))
            throw new ArgumentException($"Type '{type.Name}' is not mapped to a table name.");

        return Encapsulate(raw);
    }

    // Lock-free monotonic counter for the timestamp portion of SequentialGuid. Packed as
    // (days << 32) | tick so we can compare-and-swap with a single Interlocked op.
    private static long _lastSequentialTimestamp;

    /// <summary>
    /// Returns a new sequential GUID based on the current timestamp. The timestamp portion is
    /// guaranteed to be strictly monotonically increasing across concurrent calls — within the
    /// same SQL-Server tick (~3.33 ms) the counter advances by one tick rather than producing
    /// non-sortable duplicates.
    /// </summary>
    public static Guid SequentialGuid()
    {
        var now = DateTime.UtcNow;

        // Guard against ushort overflow in days portion (wraps after ~2079-06-06)
        if (now > MaxSequentialGuidDate)
            throw new InvalidOperationException(
                $"SequentialGuid date range exceeded (after {MaxSequentialGuidDate:yyyy-MM-dd}).");

        var days = (now - BaseDateUtc).Days;
        if ((uint)days > ushort.MaxValue)
            throw new InvalidOperationException(
                $"SequentialGuid day range exceeded (after {MaxSequentialGuidDate:yyyy-MM-dd}).");

        var tick = (int)(now.TimeOfDay.TotalMilliseconds / SqlServerTickMs);
        var candidate = ((long)days << 32) | (uint)tick;

        // Advance _lastSequentialTimestamp to max(candidate, previous + 1) atomically.
        long previous, advanced;
        do
        {
            previous = Volatile.Read(ref _lastSequentialTimestamp);
            advanced = candidate > previous ? candidate : previous + 1;
        }
        while (Interlocked.CompareExchange(ref _lastSequentialTimestamp, advanced, previous) != previous);

        var advancedDays = (int)(advanced >> 32);
        if ((uint)advancedDays > ushort.MaxValue)
            throw new InvalidOperationException("SequentialGuid sequence overflowed the day range.");

        var advancedTick = (int)(advanced & 0xFFFFFFFFL);

        var guidArray = Guid.NewGuid().ToByteArray();

        var daysArray = BitConverter.GetBytes((ushort)advancedDays);
        var msecsArray = BitConverter.GetBytes(advancedTick);

        Array.Reverse(daysArray);
        Array.Reverse(msecsArray);

        Array.Copy(daysArray, 0, guidArray, guidArray.Length - 6, 2);
        Array.Copy(msecsArray, 0, guidArray, guidArray.Length - 4, 4);

        return new Guid(guidArray);
    }

    /// <summary>
    /// Retrieves the properties of the specified type that are considered identity properties, such as those marked
    /// with a Key attribute or named "Id". Foreign <c>KeyAttribute</c> types (e.g. from
    /// <c>System.ComponentModel.DataAnnotations</c>) are accepted via name match.
    /// </summary>
    public static PropertyInfo[] GetIdProperties(Type type)
    {
        var props = type.GetProperties();
        var keyed = props.Where(IsKey).ToList();

        return (keyed.Any()
            ? keyed
            : props.Where(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase)))
            .ToArray();
    }

    private static bool IsKey(PropertyInfo p)
    {
        if (Attribute.IsDefined(p, typeof(KeyAttribute), inherit: true)) return true;
        var attrs = p.GetCustomAttributes(true);
        for (int i = 0; i < attrs.Length; i++)
            if (attrs[i].GetType().Name == "KeyAttribute") return true;
        return false;
    }

    /// <summary>
    /// Retrieves the property that represents the version of the specified type, if one exists.
    /// Throws <see cref="ArgumentException"/> if the type is misconfigured (e.g. multiple [Version]
    /// attributes or wrong property type). Returns null only when no version property is defined at all.
    /// </summary>
    public static PropertyInfo? GetVersionProperty(Type type)
        => _versionProps.TryGetValue(type, out var cached)
            ? cached
            : ResolveAndCacheVersionProperty(type);

    private static PropertyInfo? ResolveAndCacheVersionProperty(Type type)
    {
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);

        var marked = props
            .Where(p => p.GetCustomAttributes(true).Any(a => a is VersionAttribute))
            .ToArray();

        if (marked.Length > 1)
            throw new ArgumentException(
                $"{type.Name} has multiple properties marked with [{nameof(VersionAttribute)}]. Only one is allowed.");

        PropertyInfo? prop = marked.Length == 1
            ? marked[0]
            : props.FirstOrDefault(p => p.Name.Equals("Version", StringComparison.OrdinalIgnoreCase));

        if (prop == null)
            return null; // No version property — not an error; caller decides.

        if (prop.PropertyType != typeof(long))
            throw new ArgumentException(
                $"{type.Name}.{prop.Name} must be of type long (non-nullable) to use optimistic concurrency.");

        _versionProps[type] = prop;
        return prop;
    }

    /// <summary>
    /// Attempts to retrieve the version property for <paramref name="type"/>.
    /// Returns <see langword="false"/> (with <paramref name="prop"/> set to <see langword="null"/>)
    /// when the type simply has no version property.
    /// Re-throws <see cref="ArgumentException"/> for misconfigured types (multiple [Version] attributes
    /// or wrong property type) so callers are not silently swallowed.
    /// </summary>
    /// <exception cref="ArgumentNullException">Thrown if <paramref name="type"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// Thrown if the type is misconfigured (multiple [Version] attributes or wrong property type).
    /// </exception>
    public static bool TryGetVersionProperty(Type type, out PropertyInfo? prop)
    {
        if (type is null) throw new ArgumentNullException(nameof(type));

        // BUG #1 FIX: Do NOT catch ArgumentException here. Configuration errors (multiple [Version]
        // attributes, non-long type) must propagate so they are caught at startup/test time rather
        // than silently producing wrong runtime behavior.
        prop = GetVersionProperty(type); // returns null when no version property exists; throws on misconfiguration
        return prop is not null;
    }

    /// <summary>
    /// Returns the identifier properties of an entity instance.
    /// </summary>
    public static PropertyInfo[] GetIdProperties(object entity) => GetIdProperties(entity.GetType());

    private string EncapsulateToken(string identifier)
    {
        // Escape the closing delimiter inside the identifier (e.g. ] -> ]] for SQL Server).
        var escape = _dialect.IdentifierEscapeChar;
        if (identifier.IndexOf(escape) >= 0)
            identifier = identifier.Replace(escape.ToString(), new string(escape, 2));

        return string.Format(_dialect.Encapsulation, identifier);
    }
}