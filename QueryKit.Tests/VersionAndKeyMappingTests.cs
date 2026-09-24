using System.Data;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;
using DataAnnotations = System.ComponentModel.DataAnnotations;

namespace QueryKit.Tests;

/// <summary>
/// Which columns each write touches, where that depends on how the key and the version column are
/// recognised: a string property that happens to be called Version is data, a DataAnnotations
/// [Key] is as much a key as QueryKit's own, and neither an upsert nor a batch update overwrites
/// a stored version with the stale one an entity is carrying.
/// </summary>
[TestFixture]
public class VersionAndKeyMappingTests
{
    private IDbConnection _conn = null!;
    private Action<string>? _originalLogger;

    public class Release
    {
        public Guid Id { get; set; }
        public string Version { get; set; } = "";
        public string Notes { get; set; } = "";
    }

    public class Customer
    {
        [DataAnnotations.Key]
        public int CustomerId { get; set; }
        public string Name { get; set; } = "";
    }

    [SetUp]
    public void SetUp()
    {
        _conn = TestDatabase.OpenAndInit();
        _conn.Execute("CREATE TABLE Release (Id TEXT PRIMARY KEY, Version TEXT NOT NULL, Notes TEXT NOT NULL);");
        _conn.Execute("CREATE TABLE Customer (CustomerId INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
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

    // ------------------------------------------------------------ a Version that is not a counter

    [Test]
    public async Task AStringVersionIsInsertedAndUpdatedLikeAnyOtherColumn()
    {
        var release = new Release { Version = "1.2.0", Notes = "First" };
        await _conn.InsertAsync<Guid, Release>(release);

        release.Version = "1.2.1";
        release.Notes = "Patched";
        await _conn.UpdateAsync(release);

        var stored = await _conn.GetAsync<Release>(release.Id);
        Assert.That(stored!.Version, Is.EqualTo("1.2.1"));
        Assert.That(stored.Notes, Is.EqualTo("Patched"));
    }

    [Test]
    public async Task AStringVersionIsBatchInserted()
    {
        var releases = new[]
        {
            new Release { Version = "1.0.0", Notes = "a" },
            new Release { Version = "2.0.0", Notes = "b" },
        };

        await _conn.BatchInsertAsync(releases);

        var versions = (await _conn.QueryAsync<string>("SELECT Version FROM Release ORDER BY Version")).ToArray();
        Assert.That(versions, Is.EqualTo(new[] { "1.0.0", "2.0.0" }));
    }

    // ---------------------------------------------------------------- a DataAnnotations [Key]

    [Test]
    public async Task ADataAnnotationsIntKeyIsLeftToTheDatabaseOnInsert()
    {
        var sql = new List<string>();
        ConnectionExtensions.Logger = sql.Add;

        var first = await _conn.InsertAsync<int?, Customer>(new Customer { Name = "Ada" });
        var second = await _conn.InsertAsync<int?, Customer>(new Customer { Name = "Alan" });

        Assert.That(first, Is.Not.Null.And.Not.EqualTo(second));
        // The key is read back (RETURNING "CustomerId"), but never written.
        Assert.That(sql.Where(s => s.Contains("insert into")),
            Has.All.Contains("\"Customer\" (\"Name\") values (@Name)"));
    }

    [Test]
    public async Task ADataAnnotationsKeyIsNotInAnUpdatesSetClause()
    {
        var id = await _conn.InsertAsync<int?, Customer>(new Customer { Name = "Ada" });
        var sql = new List<string>();
        ConnectionExtensions.Logger = sql.Add;

        await _conn.UpdateAsync(new Customer { CustomerId = id!.Value, Name = "Ada Lovelace" });

        var update = sql.Single(s => s.Contains("update"));
        Assert.That(update.Substring(0, update.IndexOf(" where", StringComparison.Ordinal)),
            Does.Not.Contain("CustomerId"));
        Assert.That((await _conn.GetAsync<Customer>(id.Value))!.Name, Is.EqualTo("Ada Lovelace"));
    }

    // ------------------------------------------------ writes that must not rewind a stored version

    private async Task<PersonWithVersion> InsertThenBumpInTheDatabase()
    {
        var person = new PersonWithVersion { FirstName = "Ada", LastName = "Lovelace", Age = 36 };
        await _conn.InsertAsync<Guid, PersonWithVersion>(person);          // stored at version 1
        await _conn.UpdateWithVersionAsync(person, expectedVersion: 1);    // stored at version 2
        return person;                                                     // still says version 1
    }

    private Task<long> StoredVersion(Guid id) =>
        _conn.ExecuteScalarAsync<long>("SELECT Version FROM PersonWithVersion WHERE Id = @id", new { id });

    [Test]
    public async Task BatchUpdateLeavesTheStoredVersionAlone()
    {
        var stale = await InsertThenBumpInTheDatabase();
        stale.FirstName = "Augusta";

        await _conn.BatchUpdateAsync(new[] { stale });

        Assert.That(await StoredVersion(stale.Id), Is.EqualTo(2));
    }

    [Test]
    public async Task UpsertLeavesTheStoredVersionAloneWhenItUpdates()
    {
        var stale = await InsertThenBumpInTheDatabase();
        stale.FirstName = "Augusta";

        await _conn.UpsertAsync(new[] { stale });

        Assert.That(await StoredVersion(stale.Id), Is.EqualTo(2));
    }

    [Test]
    public async Task UpsertStartsAnInsertedRowAtVersionOne()
    {
        var person = new PersonWithVersion { Id = Guid.NewGuid(), FirstName = "Ada", LastName = "Lovelace", Age = 36 };

        await _conn.UpsertAsync(new[] { person });

        Assert.That(person.Version, Is.EqualTo(1));
        Assert.That(await StoredVersion(person.Id), Is.EqualTo(1));
    }
}
