using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

namespace QueryKit.Extensions
{
    internal static class TypeExtensions
    {
        internal static bool IsSimpleType(this Type type)
        {
            var underlyingType = Nullable.GetUnderlyingType(type);
            type = underlyingType ?? type;
            var simpleTypes = new List<Type>
            {
                typeof(byte), typeof(sbyte), typeof(short), typeof(ushort), typeof(int), typeof(uint),
                typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal),
                typeof(bool), typeof(string), typeof(char), typeof(Guid),
                typeof(DateTime), typeof(DateTimeOffset), typeof(TimeSpan), typeof(byte[])
            };
            return simpleTypes.Contains(type) || type.IsEnum;
        }

        internal static string CacheKey(this IEnumerable<PropertyInfo> props) =>
            string.Join(",", props.Select(p => p.DeclaringType!.FullName + "." + p.Name));
    }
}