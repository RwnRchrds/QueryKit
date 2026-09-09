using QueryKit.Dialects;

namespace QueryKit.Tests;

/// <summary>
/// Only SQLite can be executed here, so the SQL built for the other dialects is what there is to
/// check. These pin the parts that were wrong: PostgreSQL read a generated key back with LASTVAL,
/// and the Oracle and DB2 paging templates selected their own scaffolding column and left the outer
/// query unordered.
/// </summary>
[TestFixture]
public class DialectSqlTests
{
    [TestCase(Dialect.PostgreSQL, true)]
    [TestCase(Dialect.SQLite, true)]
    [TestCase(Dialect.SQLServer, false)]
    [TestCase(Dialect.MySQL, false)]
    [TestCase(Dialect.Oracle, false)]
    [TestCase(Dialect.DB2, false)]
    public void KnowsWhichDialectsCanReturnTheKeyFromTheInsert(Dialect dialect, bool expected)
    {
        Assert.That(DialectConfig.Create(dialect).SupportsInsertReturning, Is.EqualTo(expected));
    }

    [Test]
    public void PostgresDoesNotReadAGeneratedKeyBackWithLastval()
    {
        var config = DialectConfig.Create(Dialect.PostgreSQL);

        // LASTVAL() is session-wide across every sequence, so an insert whose trigger writes to
        // another table would return that table's key. RETURNING names the row just written.
        Assert.That(config.SupportsInsertReturning, Is.True,
            "PostgreSQL must use RETURNING rather than the IdentitySql fallback.");
    }

    [TestCase(Dialect.Oracle)]
    [TestCase(Dialect.DB2)]
    public void PagingDoesNotSelectItsOwnScaffoldingColumn(Dialect dialect)
    {
        var sql = DialectConfig.Create(dialect).PagedListSql;

        // These two need a row-number column to filter a page out of. It exists to be filtered and
        // ordered by; the outer query must name the caller's columns, because a star over the
        // subquery drags the scaffolding into the result set with them.
        var outerSelect = sql[..sql.IndexOf("from", StringComparison.OrdinalIgnoreCase)];

        Assert.That(outerSelect, Does.Not.Contain("*"),
            $"{dialect} selects * over its paging subquery, so PagedNumber reaches the caller.");
        Assert.That(outerSelect, Does.Contain("{SelectColumns}"),
            $"{dialect} must name the caller's columns in the outer select.");
    }

    [TestCase(Dialect.Oracle)]
    [TestCase(Dialect.DB2)]
    public void PagingOrdersTheOuterQuery(Dialect dialect)
    {
        var sql = DialectConfig.Create(dialect).PagedListSql;

        // Filtering a row-number range does not oblige the outer query to return rows in that
        // order; without this a page can come back shuffled.
        var lastOrderBy = sql.LastIndexOf("order by", StringComparison.OrdinalIgnoreCase);
        var lastBetween = sql.LastIndexOf("between", StringComparison.OrdinalIgnoreCase);
        Assert.That(lastOrderBy, Is.GreaterThan(lastBetween),
            $"{dialect} filters the page but never orders it.");
    }

    [TestCase(Dialect.SQLServer)]
    [TestCase(Dialect.PostgreSQL)]
    [TestCase(Dialect.SQLite)]
    [TestCase(Dialect.MySQL)]
    [TestCase(Dialect.Oracle)]
    [TestCase(Dialect.DB2)]
    public void EveryPagingTemplateUsesEveryPlaceholderItNeeds(Dialect dialect)
    {
        var sql = DialectConfig.Create(dialect).PagedListSql;

        foreach (var token in new[] { "{SelectColumns}", "{TableName}", "{WhereClause}", "{OrderBy}",
                     "{PageNumber}", "{RowsPerPage}" })
        {
            Assert.That(sql, Does.Contain(token), $"{dialect} paging drops {token}.");
        }
    }
}
