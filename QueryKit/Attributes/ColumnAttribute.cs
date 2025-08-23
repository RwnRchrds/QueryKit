namespace QueryKit.Attributes
{
    /// <summary>
    /// Specifies the database column name that a property maps to.
    /// </summary>
    [System.AttributeUsage(System.AttributeTargets.Property)]
    public sealed class ColumnAttribute : System.Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ColumnAttribute"/> class.
        /// </summary>
        /// <param name="columnName">The target column name in the database.</param>
        public ColumnAttribute(string columnName)
        {
            Name = columnName;
        }
        
        /// <summary>
        /// Gets the target column name in the database.
        /// </summary>
        public string Name { get; }
    }
}