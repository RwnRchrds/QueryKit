namespace QueryKit.Dialects
{
    /// <summary>
    /// How a dialect spells "insert this row, or update it if the key is already there".
    /// </summary>
    public enum UpsertStyle
    {
        /// <summary>
        /// <c>MERGE ... USING ... WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT</c>.
        /// SQL Server, Oracle and Db2.
        /// </summary>
        Merge,

        /// <summary>
        /// <c>INSERT ... ON CONFLICT (key) DO UPDATE SET col = excluded.col</c>.
        /// PostgreSQL and SQLite.
        /// </summary>
        OnConflict,

        /// <summary>
        /// <c>INSERT ... ON DUPLICATE KEY UPDATE col = VALUES(col)</c>. MySQL and MariaDB.
        /// </summary>
        OnDuplicateKey
    }
}
