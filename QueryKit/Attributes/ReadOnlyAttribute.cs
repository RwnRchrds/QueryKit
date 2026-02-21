namespace QueryKit.Attributes;

/// <summary>
/// Indicates that a property should be treated as read-only and excluded from UPDATEs,
/// but still included in INSERTs and SELECTs.
/// This is useful for properties that are set on insert but should not be modified afterward.
/// </summary>
[System.AttributeUsage(System.AttributeTargets.Property)]
public sealed class ReadOnlyAttribute : System.Attribute
{
}