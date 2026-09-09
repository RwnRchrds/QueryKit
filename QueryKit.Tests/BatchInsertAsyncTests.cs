using System.Data;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

/// <summary>
/// BatchInsertAsync exists because InsertAsync costs a round trip per entity, and on a collection
/// of any size the latency is the work. Two things are therefore worth testing: that it writes the
/// same rows a loop of InsertAsync would, and that the SQL it builds is right for each dialect —
/// Oracle in particular has no multi-row VALUES and needs INSERT ALL.
/// </summary>
[TestFixture]
public class BatchInsertAsyncTests
{
    private IDbConnection _conn = null!;
    private Action<string>? _originalLogger;

    [SetUp]
    public void SetUp()
    {
        _conn = TestDatabase.OpenAndInit();
        ConnectionExtensions.UseDialect(Dialect.SQLite);
        _originalLogger = ConnectionExtensions.Logger;
    }

    [TearDown]
    public void TearDown()
    {
        ConnectionExtensions.Logger = _originalLogger;
        ConnectionExtensions.UseDialect(Dialect.SQLServer);
        _conn.Dispose();
    }

    private static Person[] People(int count) =>
        Enumerable.Range(0, count)
            .Select(i => new Person { Id = Guid.NewGuid(), FirstName = "First" + i, LastName = "Last" + i, Age = 20 + i })
            .ToArray();

    // ----------------------------------------------------------------- behaviour, against SQLite

    [Test]
    public async Task WritesEveryRow()
    {
        var written = await _conn.BatchInsertAsync(People(50));

        Assert.That(written, Is.EqualTo(50));
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(50));
    }

    [Test]
    public async Task RoundTripsValuesUnchanged()
    {
        var people = People(3);

        await _conn.BatchInsertAsync(people);

        var back = (await _conn.QueryAsync<Person>("SELECT * FROM Persons ORDER BY Age")).ToArray();
        Assert.That(back.Select(p => p.Id), Is.EqualTo(people.Select(p => p.Id)));
        Assert.That(back.Select(p => p.FirstName), Is.EqualTo(people.Select(p => p.FirstName)));
        Assert.That(back.Select(p => p.Age), Is.EqualTo(people.Select(p => p.Age)));
    }

    [Test]
    public async Task EmptyCollectionWritesNothingAndDoesNotThrow()
    {
        Assert.That(await _conn.BatchInsertAsync(Array.Empty<Person>()), Is.EqualTo(0));
    }

    [Test]
    public async Task FillsEmptyGuidKeysPerRow()
    {
        var people = People(4);
        foreach (var p in people) p.Id = Guid.Empty;

        await _conn.BatchInsertAsync(people);

        Assert.That(people.Select(p => p.Id), Has.None.EqualTo(Guid.Empty));
        Assert.That(people.Select(p => p.Id).Distinct().Count(), Is.EqualTo(4),
            "Every row must get its own key, not one key reused across the batch.");
    }

    [Test]
    public async Task SpansSeveralStatementsWhenTheBatchSizeIsExceeded()
    {
        var statements = 0;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchInsertAsync")) statements++; };

        var written = await _conn.BatchInsertAsync(People(25), batchSize: 10);

        Assert.That(written, Is.EqualTo(25));
        Assert.That(statements, Is.EqualTo(3), "25 rows at 10 per batch is three statements.");
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(25));
    }

    [Test]
    public async Task MatchesWhatALoopOfInsertAsyncWouldWrite()
    {
        var viaLoop = People(5);
        foreach (var p in viaLoop) await _conn.InsertAsync(p);
        var loopRows = (await _conn.QueryAsync<Person>("SELECT * FROM Persons")).Count();

        await _conn.ExecuteAsync("DELETE FROM Persons");

        var written = await _conn.BatchInsertAsync(People(5));

        Assert.That(written, Is.EqualTo(loopRows));
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(loopRows));
    }

    [Test]
    public void RollsBackWithItsTransaction()
    {
        using var conn = TestDatabase.OpenAndInit();
        using (var tx = conn.BeginTransaction())
        {
            conn.BatchInsertAsync(People(10), tx).GetAwaiter().GetResult();
            tx.Rollback();
        }

        Assert.That(conn.ExecuteScalar<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(0));
    }

    // ----------------------------------------------------------------------- composite keys

    [Test]
    public async Task WritesCompositeKeysWithoutInventingAnyPart()
    {
        await _conn.ExecuteAsync(@"
            create table if not exists OrderItems (
                OrderId    TEXT    not null,
                LineNumber INTEGER not null,
                Sku        TEXT,
                primary key (OrderId, LineNumber));");

        var orderId = Guid.NewGuid();
        var items = Enumerable.Range(1, 3)
            .Select(i => new OrderItem { OrderId = orderId, LineNumber = i, Sku = "SKU" + i })
            .ToArray();

        var written = await _conn.BatchInsertAsync(items);

        Assert.That(written, Is.EqualTo(3));
        // Every part of the key must survive: a composite key has no auto-identity part, so
        // nothing may be generated over the top of what the caller supplied.
        Assert.That(items.Select(i => i.OrderId), Is.All.EqualTo(orderId));
        Assert.That(await _conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM OrderItems WHERE OrderId = @OrderId", new { OrderId = orderId }),
            Is.EqualTo(3));
        Assert.That(await _conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(DISTINCT LineNumber) FROM OrderItems"), Is.EqualTo(3));
    }

    // ------------------------------------------------------------------------- identity keys

    [Test]
    public void RefusesAnIdentityKeyRatherThanLeavingItSilentlyUnset()
    {
        var people = new[] { new AutoIntPerson { Name = "Ada" }, new AutoIntPerson { Name = "Alan" } };

        var ex = Assert.ThrowsAsync<NotSupportedException>(
            () => _conn.BatchInsertAsync(people));

        // The message has to say what to do instead, because the alternative is a caller
        // shipping rows whose Id is still 0 and reading them back as foreign keys.
        Assert.That(ex!.Message, Does.Contain("discardGeneratedKeys"));
        Assert.That(ex.Message, Does.Contain("AutoIntPerson"));
    }

    [Test]
    public async Task WritesIdentityRowsWhenTheCallerSaysTheKeysAreNotNeeded()
    {
        var people = Enumerable.Range(0, 5).Select(i => new AutoIntPerson { Name = "P" + i }).ToArray();

        var written = await _conn.BatchInsertAsync(people, discardGeneratedKeys: true);

        Assert.That(written, Is.EqualTo(5));
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM AutoIntPersons"), Is.EqualTo(5));
        // The database assigned real keys even though the entities never learned them.
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(DISTINCT Id) FROM AutoIntPersons"),
            Is.EqualTo(5));
    }

    [Test]
    public async Task LeavesTheEntitiesKeysUnsetWhenTheyAreDiscarded()
    {
        var people = Enumerable.Range(0, 3).Select(i => new AutoIntPerson { Name = "P" + i }).ToArray();

        await _conn.BatchInsertAsync(people, discardGeneratedKeys: true);

        // Stated plainly so nobody mistakes this for InsertAsync, which does populate the key.
        Assert.That(people.Select(p => p.Id), Is.All.EqualTo(0));
    }

    [Test]
    public void DoesNotPutTheIdentityColumnInTheStatement()
    {
        string? captured = null;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchInsertAsync")) captured ??= msg; };

        _conn.BatchInsertAsync(new[] { new AutoIntPerson { Name = "Ada" } }, discardGeneratedKeys: true)
            .GetAwaiter().GetResult();

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured, Does.Not.Contain("@Id_0"),
            "The identity column must be left for the database to fill.");
        Assert.That(captured, Does.Contain("@Name_0"));
    }

    // ------------------------------------------------------------- generated SQL, every dialect

    private string CaptureSql(Dialect dialect, int rows)
    {
        ConnectionExtensions.UseDialect(dialect);
        string? captured = null;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchInsertAsync")) captured ??= msg; };

        try
        {
            // Only SQLite can actually run here; the rest throw once the statement reaches the
            // provider, by which point the SQL has already been built and logged.
            _conn.BatchInsertAsync(People(rows)).GetAwaiter().GetResult();
        }
        catch
        {
            // The generated SQL is what is under test, not whether SQLite accepts another dialect.
        }

        Assert.That(captured, Is.Not.Null, $"No SQL was logged for {dialect}.");
        return captured!;
    }

    [TestCase(Dialect.SQLServer, "[Persons]")]
    [TestCase(Dialect.PostgreSQL, "\"Persons\"")]
    [TestCase(Dialect.SQLite, "\"Persons\"")]
    [TestCase(Dialect.MySQL, "`Persons`")]
    [TestCase(Dialect.DB2, "\"Persons\"")]
    public void MultiRowDialectsUseOneValuesClause(Dialect dialect, string encapsulatedTable)
    {
        var sql = CaptureSql(dialect, 3);

        Assert.That(sql, Does.Contain("insert into " + encapsulatedTable));
        Assert.That(sql, Does.Contain("values "));
        Assert.That(sql, Does.Not.Contain("insert all"));
        // Three rows, so three placeholder groups and exactly two separators between them.
        Assert.That(sql, Does.Contain("@Id_0"));
        Assert.That(sql, Does.Contain("@Id_1"));
        Assert.That(sql, Does.Contain("@Id_2"));
        Assert.That(sql.Split("), (").Length, Is.EqualTo(3), "Rows should share one VALUES clause.");
    }

    [Test]
    public void OracleUsesInsertAllBecauseItHasNoMultiRowValues()
    {
        var sql = CaptureSql(Dialect.Oracle, 3);

        Assert.That(sql, Does.Contain("insert all"));
        Assert.That(sql, Does.Contain("select 1 from dual"), "INSERT ALL needs a driving query.");
        // One INTO per row, each with its own parameters.
        Assert.That(sql.Split("into \"Persons\"").Length, Is.EqualTo(4));
        Assert.That(sql, Does.Contain("@Id_0"));
        Assert.That(sql, Does.Contain("@Id_2"));
    }

    [TestCase(Dialect.SQLServer)]
    [TestCase(Dialect.PostgreSQL)]
    [TestCase(Dialect.SQLite)]
    [TestCase(Dialect.MySQL)]
    [TestCase(Dialect.Oracle)]
    [TestCase(Dialect.DB2)]
    public void EveryDialectParameterisesEveryRowSeparately(Dialect dialect)
    {
        var sql = CaptureSql(dialect, 4);

        // A row reusing another's parameter names would silently write the same values repeatedly.
        foreach (var row in Enumerable.Range(0, 4))
        {
            Assert.That(sql, Does.Contain($"@Id_{row}"));
            Assert.That(sql, Does.Contain($"@FirstName_{row}"));
        }

        Assert.That(sql, Does.Not.Contain("@Id_4"), "Only the rows in this batch should be parameterised.");
    }

    [TestCase(Dialect.SQLServer, true)]
    [TestCase(Dialect.PostgreSQL, true)]
    [TestCase(Dialect.SQLite, true)]
    [TestCase(Dialect.MySQL, true)]
    [TestCase(Dialect.DB2, true)]
    [TestCase(Dialect.Oracle, false)]
    public void DialectConfigKnowsWhichDialectsSupportMultiRowValues(Dialect dialect, bool expected)
    {
        Assert.That(DialectConfig.Create(dialect).SupportsMultiRowValues, Is.EqualTo(expected));
    }
}
