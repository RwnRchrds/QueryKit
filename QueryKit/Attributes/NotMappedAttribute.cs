namespace QueryKit.Attributes
{
    /// <summary>
    /// Indicates that a property is not mapped to a database column.
    /// Such properties are ignored for SELECT/INSERT/UPDATE.
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public sealed class NotMappedAttribute : System.Attribute
    {
    }
}