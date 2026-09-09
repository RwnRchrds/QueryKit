namespace QueryKit.Dialects
{
    /// <summary>
    /// Supported SQL dialects used by QueryKit to format identifiers,
    /// retrieve identity values, and generate paging queries.
    /// </summary>
    public enum Dialect
    {
        /// <summary>Microsoft SQL Server and Azure SQL.</summary>
        SQLServer,

        /// <summary>PostgreSQL.</summary>
        PostgreSQL,

        /// <summary>SQLite.</summary>
        SQLite,

        /// <summary>MySQL and MariaDB.</summary>
        MySQL,

        /// <summary>Oracle Database. The only dialect here with no multi-row VALUES clause.</summary>
        Oracle,

        /// <summary>IBM Db2.</summary>
        DB2
    }
}
