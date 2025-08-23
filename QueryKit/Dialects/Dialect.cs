namespace QueryKit.Dialects
{
    /// <summary>
    /// Supported SQL dialects used by QueryKit to format identifiers,
    /// retrieve identity values, and generate paging queries.
    /// </summary>
    public enum Dialect
    {
        SQLServer,
        PostgreSQL,
        SQLite,
        MySQL,
        Oracle,
        DB2
    }
}