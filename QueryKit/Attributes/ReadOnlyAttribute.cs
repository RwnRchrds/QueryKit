namespace QueryKit.Attributes
{
    /// <summary>
    /// Indicates that a property should be treated as read-only and excluded from updates.
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public sealed class ReadOnlyAttribute : System.Attribute
    {
        public ReadOnlyAttribute(bool isReadOnly)
        {
            IsReadOnly = isReadOnly;
        }

        public bool IsReadOnly { get; }
    }
}