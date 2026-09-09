using System.Data;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

/// <summary>
/// Rows differ from one another, so a batch update is many statements in one command rather than
/// one statement over many rows. What matters is that each row gets its own values and its own key,
/// and that the whole batch costs one round trip.
/// </summary>
[TestFixture]
public class BatchUpdateAsyncTests
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

    private async Task<Person[]> Seed(int count)
    {
        var people = Enumerable.Range(0, count)
            .Select(i => new Person
            {
                Id = Guid.NewGuid(), FirstName = "First" + i, LastName = "Last" + i, Age = 20 + i
            })
            .ToArray();

        await _conn.BatchInsertAsync(people);
        return people;
    }

    [Test]
    public async Task GivesEveryRowItsOwnValues()
    {
        var people = await Seed(4);
        for (var i = 0; i < people.Length; i++)
        {
            people[i].FirstName = "Changed" + i;
            people[i].Age = 90 + i;
        }

        var affected = await _conn.BatchUpdateAsync(people);

        Assert.That(affected, Is.EqualTo(4));
        foreach (var p in people)
        {
            var back = await _conn.QuerySingleAsync<Person>(
                "SELECT * FROM Persons WHERE Id = @Id", new { p.Id });
            Assert.That(back.FirstName, Is.EqualTo(p.FirstName));
            Assert.That(back.Age, Is.EqualTo(p.Age));
        }
    }

    [Test]
    public async Task LeavesRowsOutsideTheBatchAlone()
    {
        var people = await Seed(3);
        var untouched = people[2];
        var originalName = untouched.FirstName;

        people[0].FirstName = "Changed";
        await _conn.BatchUpdateAsync(new[] { people[0] });

        Assert.That(await _conn.ExecuteScalarAsync<string>(
            "SELECT FirstName FROM Persons WHERE Id = @Id", new { untouched.Id }),
            Is.EqualTo(originalName));
    }

    [Test]
    public async Task MatchesWhatALoopOfUpdateAsyncWouldWrite()
    {
        var viaLoop = await Seed(3);
        foreach (var p in viaLoop) p.Age = 50;
        foreach (var p in viaLoop) await _conn.UpdateAsync(p);
        var loopAges = await _conn.QueryAsync<int>("SELECT Age FROM Persons ORDER BY FirstName");

        await _conn.ExecuteAsync("DELETE FROM Persons");

        var viaBatch = await Seed(3);
        foreach (var p in viaBatch) p.Age = 50;
        await _conn.BatchUpdateAsync(viaBatch);
        var batchAges = await _conn.QueryAsync<int>("SELECT Age FROM Persons ORDER BY FirstName");

        Assert.That(batchAges, Is.EqualTo(loopAges));
    }

    [Test]
    public async Task EmptyCollectionWritesNothingAndDoesNotThrow()
    {
        Assert.That(await _conn.BatchUpdateAsync(Array.Empty<Person>()), Is.EqualTo(0));
    }

    [Test]
    public async Task SpansSeveralCommandsWhenTheBatchSizeIsExceeded()
    {
        var commands = 0;
        var people = await Seed(25);
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchUpdateAsync")) commands++; };

        foreach (var p in people) p.Age = 99;
        var affected = await _conn.BatchUpdateAsync(people, batchSize: 10);

        Assert.That(affected, Is.EqualTo(25));
        Assert.That(commands, Is.EqualTo(3), "25 rows at 10 per command is three commands.");
    }

    [Test]
    public async Task RollsBackWithItsTransaction()
    {
        var people = await Seed(3);
        var original = people[0].FirstName;

        using (var tx = _conn.BeginTransaction())
        {
            people[0].FirstName = "Changed";
            await _conn.BatchUpdateAsync(new[] { people[0] }, tx);
            tx.Rollback();
        }

        Assert.That(await _conn.ExecuteScalarAsync<string>(
            "SELECT FirstName FROM Persons WHERE Id = @Id", new { people[0].Id }), Is.EqualTo(original));
    }

    [Test]
    public async Task DoesNotWriteTheKeyItMatchesOn()
    {
        var people = await Seed(1);
        string? captured = null;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchUpdateAsync")) captured ??= msg; };

        people[0].Age = 77;
        await _conn.BatchUpdateAsync(people);

        // "set Id = ..." would let a typo move a row onto another row's key.
        Assert.That(captured, Is.Not.Null);
        var setClause = captured![captured.IndexOf("set ", StringComparison.Ordinal)..
                                  captured.IndexOf(" where ", StringComparison.Ordinal)];
        Assert.That(setClause, Does.Not.Contain("Id"));
    }

    [Test]
    public void OracleWrapsItsBatchInABlockBecauseItHasNoMultiStatementCommand()
    {
        ConnectionExtensions.UseDialect(Dialect.Oracle);
        string? captured = null;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchUpdateAsync")) captured ??= msg; };

        var people = Enumerable.Range(0, 2)
            .Select(i => new Person { Id = Guid.NewGuid(), FirstName = "F", LastName = "L", Age = i })
            .ToArray();

        try { _conn.BatchUpdateAsync(people).GetAwaiter().GetResult(); }
        catch { /* SQLite cannot run Oracle SQL; the built statement is what is under test. */ }

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured, Does.Contain("begin "));
        Assert.That(captured, Does.Contain("end;"));
    }

    [TestCase(Dialect.SQLServer)]
    [TestCase(Dialect.PostgreSQL)]
    [TestCase(Dialect.SQLite)]
    [TestCase(Dialect.MySQL)]
    [TestCase(Dialect.DB2)]
    public void NonOracleDialectsSendPlainStatements(Dialect dialect)
    {
        ConnectionExtensions.UseDialect(dialect);
        string? captured = null;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("BatchUpdateAsync")) captured ??= msg; };

        var people = Enumerable.Range(0, 2)
            .Select(i => new Person { Id = Guid.NewGuid(), FirstName = "F", LastName = "L", Age = i })
            .ToArray();

        try { _conn.BatchUpdateAsync(people).GetAwaiter().GetResult(); }
        catch { /* only the generated SQL is under test */ }

        Assert.That(captured, Is.Not.Null);
        Assert.That(captured, Does.Not.Contain("begin "));
        // Each row carries its own parameters, or every row would be written the same values.
        Assert.That(captured, Does.Contain("@Age_0"));
        Assert.That(captured, Does.Contain("@Age_1"));
        Assert.That(captured, Does.Contain("@Id_0"));
        Assert.That(captured, Does.Contain("@Id_1"));
    }
}
