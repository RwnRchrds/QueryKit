using System;

namespace QueryKit.Attributes;

/// <summary>
/// Marks a property as the optimistic concurrency version column.
/// </summary>
[AttributeUsage(AttributeTargets.Property)]
public sealed class VersionAttribute : Attribute
{
}