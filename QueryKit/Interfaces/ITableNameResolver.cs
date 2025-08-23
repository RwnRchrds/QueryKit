using System;

namespace QueryKit.Interfaces
{
    /// <summary>
    /// Resolves the database table name for a given CLR type.
    /// Implementations may use attributes, naming conventions, or other metadata.
    /// </summary>
    public interface ITableNameResolver
    {
        /// <summary>
        /// Resolves the mapped table name for the specified entity <paramref name="type"/>.
        /// </summary>
        /// <param name="type">The entity CLR type.</param>
        /// <returns>The table name to use in SQL statements.</returns>
        string ResolveTableName(Type type);
    }
}