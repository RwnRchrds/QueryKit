namespace QueryKit.Attributes
{
    /// <summary>
    /// Marks a property as part of the primary key. If no property is marked,
    /// QueryKit falls back to a property named "Id" (case-insensitive).
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public sealed class KeyAttribute : System.Attribute
    {
        
    }
}