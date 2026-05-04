using System;
using System.Collections.Generic;

namespace QueryKit.Extensions
{
    internal static class TypeExtensions
    {
        private static readonly HashSet<Type> SimpleTypes = new()
        {
            typeof(byte), typeof(sbyte), typeof(short), typeof(ushort), typeof(int), typeof(uint),
            typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(decimal),
            typeof(bool), typeof(string), typeof(char), typeof(Guid),
            typeof(DateTime), typeof(DateTimeOffset), typeof(TimeSpan), typeof(byte[])
        };

        internal static bool IsSimpleType(this Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            return SimpleTypes.Contains(type) || type.IsEnum;
        }
    }
}