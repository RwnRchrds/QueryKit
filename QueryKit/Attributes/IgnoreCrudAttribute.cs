namespace QueryKit.Attributes;

/// <summary>
/// Specifies that a property should be excluded from automatic CRUD (Create, Read, Update, Delete) operations.
/// </summary>
/// <remarks>Apply this attribute to a property to prevent it from being included in data persistence or
/// mapping processes that use reflection to perform CRUD operations. This is typically used in data models where
/// certain properties should not be stored in or retrieved from a database.</remarks>
[System.AttributeUsage(System.AttributeTargets.Property)]
public sealed class IgnoreCrudAttribute : System.Attribute
{
}