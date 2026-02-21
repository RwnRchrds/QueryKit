using System;

namespace QueryKit.Attributes;

/// <summary>
/// Specifies the database table that a class is mapped to when using QueryKit.
/// </summary>
/// <remarks>Apply this attribute to a class to indicate the corresponding table name and, optionally, the schema
/// in the database. This is commonly used in frameworks such as Entity Framework to customize the mapping between a
/// class and a database table. If the schema is not specified, the default schema for the database provider is
/// used.</remarks>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class TableAttribute : Attribute
{
    /// <summary>
    /// Gets the name of the table associated with this entity.
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// Gets the name of the database schema associated with this entity.
    /// </summary>
    public string? Schema { get; set; }

    /// <summary>
    /// Initializes a new instance of the TableAttribute class using the specified table name.
    /// </summary>
    /// <param name="name">The name of the database table to associate with the attributed class. Cannot be null or empty.</param>
    public TableAttribute(string name) => Name = name;

    /// <summary>
    /// Initializes a new instance of the TableAttribute class with the specified table name and schema.
    /// </summary>
    /// <param name="name">The name of the database table to which the attribute applies. Cannot be null or empty.</param>
    /// <param name="schema">The schema that contains the table. Cannot be null or empty.</param>
    public TableAttribute(string name, string schema)
    {
        Name = name;
        Schema = schema;
    }
}