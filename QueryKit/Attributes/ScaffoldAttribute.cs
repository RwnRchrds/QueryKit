using System;

namespace QueryKit.Attributes;

/// <summary>
/// Marks a property as a database column even though its type is not one QueryKit treats as
/// simple. Without it such a property is left out of every generated statement, so a value set
/// on it is silently dropped on insert and never read back.
/// </summary>
/// <remarks>
/// The usual reason to need this is a type the CLR gained after the simple-type list was written,
/// such as <c>TimeOnly</c> or <c>DateOnly</c>. Use <c>[NotMapped]</c> or <c>[IgnoreCrud]</c> for
/// the opposite case, a property that must stay out of SQL.
/// </remarks>
[AttributeUsage(AttributeTargets.Property)]
public sealed class ScaffoldAttribute : Attribute
{
}
