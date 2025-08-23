namespace QueryKit.Attributes
{
    /// <summary>
    /// Indicates that a property value is required on INSERT.
    /// Used by QueryKit to decide whether to include key properties
    /// and to validate input for string keys.
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public sealed class RequiredAttribute : System.Attribute
    {
        
    }
}