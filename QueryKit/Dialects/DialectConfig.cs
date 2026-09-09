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

        /// <summary>
        /// Gets the closing delimiter used to encapsulate identifiers. Occurrences of this
        /// character inside an identifier must be doubled to escape (e.g. <c>]</c> → <c>]]</c>
        /// for SQL Server, <c>"</c> → <c>""</c> for ANSI/Postgres/SQLite/Oracle/DB2,
        /// <c>`</c> → <c>``</c> for MySQL).
        /// </summary>
        public char IdentifierEscapeChar { get; }

        /// <summary>
        /// Gets a value indicating whether the dialect accepts several rows in one VALUES clause —
        /// <c>INSERT INTO t (a, b) VALUES (1, 2), (3, 4)</c>.
        /// </summary>
        /// <remarks>
        /// True everywhere QueryKit supports except Oracle, which has no multi-row VALUES and
        /// spells the same thing <c>INSERT ALL INTO t (a, b) VALUES (1, 2) INTO t (a, b)
        /// VALUES (3, 4) SELECT 1 FROM dual</c>. Batch inserts consult this rather than assuming
        /// the SQL Server form.
        /// </remarks>
        public bool SupportsMultiRowValues { get; }

        /// <summary>
        /// Gets a value indicating whether an insert can name its generated key with a
        /// <c>RETURNING</c> clause instead of reading it back with a second statement.
        /// </summary>
        /// <remarks>
        /// Where it exists it is the only correct option. PostgreSQL's <c>LASTVAL()</c> returns the
        /// last value taken from <em>any</em> sequence in the session, so inserting into a table
        /// whose trigger writes elsewhere hands back the other table's key.
        /// </remarks>
        public bool SupportsInsertReturning { get; }

        /// <summary>
        /// Gets how this dialect spells an upsert. See <see cref="UpsertStyle"/>.
        /// </summary>
        public UpsertStyle UpsertStyle { get; }

        private DialectConfig(Dialect dialect, string encap, string identitySql, string pagedSql)
        {
            Dialect = dialect; Encapsulation = encap; IdentitySql = identitySql; PagedListSql = pagedSql;
            // The closing delimiter is the last character of the format template.
            IdentifierEscapeChar = encap.Length > 0 ? encap[encap.Length - 1] : '"';
            SupportsMultiRowValues = dialect != Dialect.Oracle;
            SupportsInsertReturning = dialect == Dialect.PostgreSQL || dialect == Dialect.SQLite;

            switch (dialect)
            {
                case Dialect.PostgreSQL:
                case Dialect.SQLite:
                    UpsertStyle = UpsertStyle.OnConflict;
                    break;
                case Dialect.MySQL:
                    UpsertStyle = UpsertStyle.OnDuplicateKey;
                    break;
                default:
                    UpsertStyle = UpsertStyle.Merge;
                    break;
            }
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
                        "SELECT LASTVAL() AS id",
                        "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})");
                case Dialect.SQLite:
                    return new DialectConfig(dialect, "\"{0}\"",
                        "SELECT LAST_INSERT_ROWID() AS id",
                        "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})");
                case Dialect.MySQL:
                    return new DialectConfig(dialect, "`{0}`",
                        "SELECT LAST_INSERT_ID() AS id",
                        "Select {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy} LIMIT {RowsPerPage} OFFSET (({PageNumber}-1) * {RowsPerPage})");
                case Dialect.Oracle:
                    // The paging column is filtered on and then ordered by, but never selected: it
                    // is scaffolding, and letting it into the result set gives every consumer a
                    // column that is not on the entity. The outer ORDER BY is not redundant — the
                    // range filter alone does not oblige the outer query to preserve row order.
                    return new DialectConfig(dialect, "\"{0}\"",
                        "",
                        "SELECT {SelectColumns} FROM (SELECT ROWNUM PagedNumber, u.* FROM (SELECT {SelectColumns} from {TableName} {WhereClause} Order By {OrderBy}) u) WHERE PagedNumber BETWEEN (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage}) ORDER BY PagedNumber");
                case Dialect.DB2:
                    return new DialectConfig(dialect, "\"{0}\"",
                        "SELECT CAST(IDENTITY_VAL_LOCAL() AS DEC(31,0)) AS \"id\" FROM SYSIBM.SYSDUMMY1",
                        "Select {SelectColumns} from (Select {SelectColumns}, row_number() over(order by {OrderBy}) as PagedNumber from {TableName} {WhereClause}) as t where t.PagedNumber between (({PageNumber}-1) * {RowsPerPage} + 1) AND ({PageNumber} * {RowsPerPage}) order by t.PagedNumber");
                default:
                    // SQL Server 2012+: OFFSET / FETCH NEXT — no PagedNumber leak in result set.
                    return new DialectConfig(dialect, "[{0}]",
                        "SELECT CAST(SCOPE_IDENTITY() AS BIGINT) AS [id]",
                        "SELECT {SelectColumns} FROM {TableName} {WhereClause} ORDER BY {OrderBy} OFFSET (({PageNumber}-1) * {RowsPerPage}) ROWS FETCH NEXT {RowsPerPage} ROWS ONLY");
            }
        }
    }
}