namespace QueryKit.Dialects
{
    /// <summary>
    /// Provides per-dialect SQL templates and identifier formatting used by QueryKit.
    /// </summary>
    public sealed class DialectConfig
    {
        /// <summary>
        /// Gets the dialect this configuration represents.
        /// </summary>
        public Dialect Dialect { get; }
        
        /// <summary>
        /// Gets the identifier encapsulation format string for the dialect
        /// (e.g., <c>"[{0}]"</c> for SQL Server, <c>"\"{0}\""</c> for PostgreSQL).
        /// </summary>
        public string Encapsulation { get; }
        
        /// <summary>
        /// Gets the SQL snippet used to fetch the last generated identity value
        /// (e.g., <c>SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]</c> on SQL Server).
        /// Empty when the dialect does not use identity retrieval (e.g., Guid/string keys).
        /// </summary>
        public string IdentitySql { get; }
        
        /// <summary>
        /// Gets the paging SQL template for the dialect. The template contains placeholders
        /// like <c>{SelectColumns}</c>, <c>{TableName}</c>, <c>{WhereClause}</c>,
        /// <c>{OrderBy}</c>, <c>{PageNumber}</c>, and <c>{RowsPerPage}</c>.
        /// Empty if paging is not supported by the dialect.
        /// </summary>
        public string PagedListSql { get; }

        private DialectConfig(Dialect dialect, string encap, string identitySql, string pagedSql)
        {
            Dialect = dialect; Encapsulation = encap; IdentitySql = identitySql; PagedListSql = pagedSql;
        }

        /// <summary>
        /// Creates a new <see cref="DialectConfig"/> using built-in defaults for the specified <see cref="Dialect"/>.
        /// </summary>
        /// <param name="dialect">The target SQL dialect.</param>
        /// <returns>A configured <see cref="DialectConfig"/> instance.</returns>
        public static DialectConfig Create(Dialect dialect)
        {
            switch (dialect)
            {
                case Dialect.PostgreSQL:
                    return new DialectConfig(dialect, "\"{0}\"",
                        "SELECT LASTVAL) AS id".Replace(")", "(") /* keep exact return 'id' */,
                        "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})");
                case Dialect.SQLite:
                    return new DialectConfig(dialect, "\"{0}\"",
                        "SELECT LAST_INSERT_ROWID() AS id",
                        "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})");
                case Dialect.MySQL:
                    return new DialectConfig(dialect, "`{0}`",
                        "SELECT LAST_INSERT_ID() AS id",
                        "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {Offset},{RowsPerPage}");
                case Dialect.Oracle:
                    return new DialectConfig(dialect, "\"{0}\"",
                        "",
                        "SELECT * FROM (SELECT ROWNUM PagedNUMBER, u.* FROM(SELECT {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy}) u) WHERE PagedNUMBER BETWEEN (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage})");
                case Dialect.DB2:
                    return new DialectConfig(dialect, "\"{0}\"",
                        "SELECT CAST(IDENTITY_VAL_LOCAL() AS DEC(31,0)) AS \"id\" FROM SYSIBM.SYSDUMMY1",
                        "Select * from (Select {SelectColumns}, row_number() over(order by {OrderBy}) as PagedNumber from {TableName} {WhereClause} Order By {OrderBy}) as t where t.PagedNumber between (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage})");
                default:
                    return new DialectConfig(dialect, "[{0}]",
                        "SELECT CAST(SCOPE_IDENTITY()  AS BIGINT) AS [id]",
                        "SELECT * FROM (SELECT ROW_NUMBER() OVER(ORDER BY {OrderBy}) AS PagedNumber, {SelectColumns} FROM {TableName} {WhereClause}) AS u WHERE PagedNumber BETWEEN (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage})");
            }
        }
    }
}