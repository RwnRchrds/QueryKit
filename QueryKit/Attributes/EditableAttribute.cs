namespace QueryKit.Attributes
{
    /// <summary>
    /// Indicates whether a property is allowed to be updated or scaffolded.
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public sealed class EditableAttribute : System.Attribute
    {
        public EditableAttribute(bool isEditable)
        {
            AllowEdit = isEditable;
        }

        public bool AllowEdit { get; }
    }
}