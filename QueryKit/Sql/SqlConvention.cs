using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Reflection;
using QueryKit.Attributes;
using QueryKit.Dialects;
using QueryKit.Interfaces;

namespace QueryKit.Sql
{
    internal sealed class SqlConvention
    {
        private readonly DialectConfig _dialect;
        private readonly ITableNameResolver _table;
        private readonly IColumnNameResolver _column;

        private readonly ConcurrentDictionary<Type,string> _tableNames = new ConcurrentDictionary<Type, string>();
        private readonly ConcurrentDictionary<string,string> _columnNames = new ConcurrentDictionary<string,string>();

        internal SqlConvention(DialectConfig dialect, ITableNameResolver tableResolver, IColumnNameResolver columnResolver)
        { _dialect = dialect; _table = tableResolver; _column = columnResolver; }

        internal string Encapsulate(string identifier) => string.Format(_dialect.Encapsulation, identifier);

        internal string GetTableName(Type type) =>
            _tableNames.GetOrAdd(type, t => _table.ResolveTableName(t));

        internal string GetTableName(object entity) => GetTableName(entity.GetType());

        internal string GetColumnName(PropertyInfo pi)
        {
            var key = $"{pi.DeclaringType}.{pi.Name}";
            return _columnNames.GetOrAdd(key, _ => _column.ResolveColumnName(pi));
        }

        internal static Guid SequentialGuid()
        {
            var tempGuid = Guid.NewGuid();
            var bytes = tempGuid.ToByteArray();
            var time = DateTime.Now;
            bytes[3] = (byte)time.Year; bytes[2] = (byte)time.Month; bytes[1] = (byte)time.Day; bytes[0] = (byte)time.Hour;
            bytes[5] = (byte)time.Minute; bytes[4] = (byte)time.Second;
            return new Guid(bytes);
        }

        internal static bool IsEditable(PropertyInfo pi)
        {
            var attrs = pi.GetCustomAttributes(false);
            if (attrs.Length > 0)
            {
                dynamic? write = attrs.FirstOrDefault(x => x.GetType().Name == nameof(EditableAttribute));
                if (write != null) return write.AllowEdit;
            }
            return false;
        }

        internal static bool IsReadOnly(PropertyInfo pi)
        {
            var attrs = pi.GetCustomAttributes(false);
            if (attrs.Length > 0)
            {
                dynamic? ro = attrs.FirstOrDefault(x => x.GetType().Name == nameof(ReadOnlyAttribute));
                if (ro != null) return ro.IsReadOnly;
            }
            return false;
        }

        internal static PropertyInfo[] GetIdProperties(Type type)
        {
            var keyed = type.GetProperties().Where(p => p.GetCustomAttributes(true).Any(a => a.GetType().Name == nameof(KeyAttribute))).ToList();
            return (keyed.Any() ? keyed : type.GetProperties().Where(p => p.Name.Equals("Id", StringComparison.OrdinalIgnoreCase))).ToArray();
        }

        internal static PropertyInfo[] GetIdProperties(object entity) => GetIdProperties(entity.GetType());
    }
}