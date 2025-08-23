using System.Reflection;

namespace QueryKit.Interfaces
{
    /// <summary>
    /// Resolves the database column name for a given property.
    /// Implementations may use attributes, naming conventions, or other metadata.
    /// </summary>
    public interface IColumnNameResolver
    {
        /// <summary>
        /// Resolves the mapped column name for the specified property.
        /// </summary>
        /// <param name="propertyInfo">The property to resolve.</param>
        /// <returns>The column name to use in SQL statements.</returns>
        string ResolveColumnName(PropertyInfo propertyInfo);
    }
}