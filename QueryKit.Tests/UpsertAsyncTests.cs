using System.Data;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

/// <summary>
/// UpsertAsync is one statement that inserts or overwrites depending on whether the key is already
/// there. SQLite runs it here, which covers the behaviour; the shape for the other dialects is
/// checked through the logged SQL, because the three spellings are genuinely different — MERGE,
/// ON CONFLICT and ON DUPLICATE KEY.
/// </summary>
[TestFixture]
public class UpsertAsyncTests
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

    private static Person Person(Guid id, string first, int age) =>
        new() { Id = id, FirstName = first, LastName = "Lovelace", Age = age };

    // ----------------------------------------------------------------- behaviour, against SQLite

    [Test]
    public async Task InsertsWhenTheKeyIsNew()
    {
        var id = Guid.NewGuid();

        var affected = await _conn.UpsertAsync(new[] { Person(id, "Ada", 36) });

        Assert.That(affected, Is.EqualTo(1));
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(1));
    }

    [Test]
    public async Task OverwritesWhenTheKeyIsAlreadyThere()
    {
        var id = Guid.NewGuid();
        await _conn.UpsertAsync(new[] { Person(id, "Ada", 36) });

        await _conn.UpsertAsync(new[] { Person(id, "Augusta", 37) });

        // One row, not two: the key matched, so the row was updated in place.
        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(1));
        Assert.That(await _conn.ExecuteScalarAsync<string>(
            "SELECT FirstName FROM Persons WHERE Id = @Id", new { Id = id }), Is.EqualTo("Augusta"));
        Assert.That(await _conn.ExecuteScalarAsync<int>(
            "SELECT Age FROM Persons WHERE Id = @Id", new { Id = id }), Is.EqualTo(37));
    }

    [Test]
    public async Task HandlesAMixOfNewAndExistingInOneCall()
    {
        var existing = Guid.NewGuid();
        await _conn.UpsertAsync(new[] { Person(existing, "Ada", 36) });

        var batch = new[]
        {
            Person(existing, "Augusta", 37),
            Person(Guid.NewGuid(), "Alan", 41),
            Person(Guid.NewGuid(), "Grace", 85),
        };

        await _conn.UpsertAsync(batch);

        Assert.That(await _conn.ExecuteScalarAsync<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(3));
        Assert.That(await _conn.ExecuteScalarAsync<string>(
            "SELECT FirstName FROM Persons WHERE Id = @Id", new { Id = existing }), Is.EqualTo("Augusta"));
    }

    [Test]
    public async Task FillsEmptyGuidKeysSoTheCallerCanSeeWhatWasWritten()
    {
        var person = Person(Guid.Empty, "Ada", 36);

        await _conn.UpsertAsync(new[] { person });

        Assert.That(person.Id, Is.Not.EqualTo(Guid.Empty));
        Assert.That(await _conn.ExecuteScalarAsync<int>(
            "SELECT COUNT(*) FROM Persons WHERE Id = @Id", new { Id = person.Id }), Is.EqualTo(1));
    }

    [Test]
    public async Task EmptyCollectionWritesNothingAndDoesNotThrow()
    {
        Assert.That(await _conn.UpsertAsync(Array.Empty<Person>()), Is.EqualTo(0));
    }

    [Test]
    public void RollsBackWithItsTransaction()
    {
        using var conn = TestDatabase.OpenAndInit();
        using (var tx = conn.BeginTransaction())
        {
            conn.UpsertAsync(new[] { Person(Guid.NewGuid(), "Ada", 36) }, tx).GetAwaiter().GetResult();
            tx.Rollback();
        }

        Assert.That(conn.ExecuteScalar<int>("SELECT COUNT(*) FROM Persons"), Is.EqualTo(0));
    }

    [Test]
    public void RefusesAnIdentityKeyBecauseItCannotIdentifyAnExistingRow()
    {
        var ex = Assert.ThrowsAsync<NotSupportedException>(
            () => _conn.UpsertAsync(new[] { new AutoIntPerson { Name = "Ada" } }));

        Assert.That(ex!.Message, Does.Contain("AutoIntPerson"));
    }

    // ------------------------------------------------------------- generated SQL, every dialect

    private string CaptureSql(Dialect dialect, int rows)
    {
        ConnectionExtensions.UseDialect(dialect);
        string? captured = null;
        ConnectionExtensions.Logger = msg => { if (msg.Contains("UpsertAsync")) captured ??= msg; };

        try
        {
            var people = Enumerable.Range(0, rows)
                .Select(i => Person(Guid.NewGuid(), "P" + i, 20 + i)).ToArray();
            _conn.UpsertAsync(people).GetAwaiter().GetResult();
        }
        catch
        {
            // Only SQLite can run here; the rest throw at the provider, by which point the SQL
            // under test has been built and logged.
        }

        Assert.That(captured, Is.Not.Null, $"No SQL was logged for {dialect}.");
        return captured!;
    }

    [TestCase(Dialect.SQLServer)]
    [TestCase(Dialect.Oracle)]
    [TestCase(Dialect.DB2)]
    public void MergeDialectsMatchOnTheKeyAndHandleBothHalves(Dialect dialect)
    {
        var sql = CaptureSql(dialect, 2);

        Assert.That(sql, Does.Contain("merge into"));
        Assert.That(sql, Does.Contain("when matched then update set"));
        Assert.That(sql, Does.Contain("when not matched then insert"));
        // Matching on anything but the key would update the wrong row.
        Assert.That(sql, Does.Match(@"on target\.\W?Id\W? = source\.\W?Id\W?"));
        // The key is what identified the row; overwriting it with itself is pointless noise.
        Assert.That(sql, Does.Not.Match(@"update set[^,]*\WId\W? = source"));
    }

    [TestCase(Dialect.PostgreSQL)]
    [TestCase(Dialect.SQLite)]
    public void OnConflictDialectsNameTheConflictTargetAndTakeTheExcludedRow(Dialect dialect)
    {
        var sql = CaptureSql(dialect, 2);

        Assert.That(sql, Does.Contain("on conflict"));
        Assert.That(sql, Does.Contain("do update set"));
        Assert.That(sql, Does.Contain("excluded."));
        Assert.That(sql, Does.Not.Contain("merge into"));
    }

    [Test]
    public void MySqlUsesOnDuplicateKeyAndTakesNoConflictTarget()
    {
        var sql = CaptureSql(Dialect.MySQL, 2);

        Assert.That(sql, Does.Contain("on duplicate key update"));
        // MySQL uses whichever unique index was violated, so naming one is a syntax error.
        Assert.That(sql, Does.Not.Contain("on conflict"));
        Assert.That(sql, Does.Contain("values("));
    }

    [Test]
    public void OracleBuildsItsSourceFromDualBecauseItHasNoMultiRowValues()
    {
        var sql = CaptureSql(Dialect.Oracle, 3);

        Assert.That(sql, Does.Contain("from dual"));
        Assert.That(sql, Does.Contain("union all"));
        Assert.That(sql.Split("union all").Length, Is.EqualTo(3), "Three rows means two unions.");
    }

    [TestCase(Dialect.SQLServer)]
    [TestCase(Dialect.PostgreSQL)]
    [TestCase(Dialect.SQLite)]
    [TestCase(Dialect.MySQL)]
    [TestCase(Dialect.Oracle)]
    [TestCase(Dialect.DB2)]
    public void EveryDialectParameterisesEveryRowSeparately(Dialect dialect)
    {
        var sql = CaptureSql(dialect, 3);

        foreach (var row in Enumerable.Range(0, 3))
        {
            Assert.That(sql, Does.Contain($"@Id_{row}"));
            Assert.That(sql, Does.Contain($"@FirstName_{row}"));
        }

        Assert.That(sql, Does.Not.Contain("@Id_3"));
    }

    [TestCase(Dialect.SQLServer, UpsertStyle.Merge)]
    [TestCase(Dialect.Oracle, UpsertStyle.Merge)]
    [TestCase(Dialect.DB2, UpsertStyle.Merge)]
    [TestCase(Dialect.PostgreSQL, UpsertStyle.OnConflict)]
    [TestCase(Dialect.SQLite, UpsertStyle.OnConflict)]
    [TestCase(Dialect.MySQL, UpsertStyle.OnDuplicateKey)]
    public void DialectConfigKnowsHowEachDialectSpellsAnUpsert(Dialect dialect, UpsertStyle expected)
    {
        Assert.That(DialectConfig.Create(dialect).UpsertStyle, Is.EqualTo(expected));
    }
}
