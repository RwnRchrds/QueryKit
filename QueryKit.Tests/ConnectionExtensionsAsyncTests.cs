using System.Data;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

[TestFixture]
public class ConnectionExtensionsAsyncTests
{
    private IDbConnection _conn = null!;

    [SetUp]
    public void SetUp()
    {
        _conn = TestDatabase.OpenAndInit();
        ConnectionExtensions.UseDialect(Dialect.SQLite);
    }

    [TearDown]
    public void TearDown() => _conn.Dispose();

    [Test]
    public async Task InsertAsync_WithNewEntity_GeneratesGuidKeyAndSetsIdOnEntity()
    {
        var p = new Person { Id = Guid.Empty, FirstName = "Barbara", LastName = "Liskov", Age = 55 };

        var key = await _conn.InsertAsync<Guid, Person>(p);

        Assert.That(key, Is.Not.EqualTo(Guid.Empty));
        Assert.That(p.Id, Is.EqualTo(key));
    }

    [Test]
    public async Task GetAsync_WithExistingId_ReturnsMatchingEntity()
    {
        var p = await SeedAsync("Barbara", "Liskov", 55);

        var loaded = await _conn.GetAsync<Person>(p.Id);

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.FirstName, Is.EqualTo("Barbara"));
        Assert.That(loaded.LastName,  Is.EqualTo("Liskov"));
        Assert.That(loaded.Age,       Is.EqualTo(55));
    }

    [Test]
    public async Task GetAsync_WithMissingId_ReturnsNull()
    {
        var loaded = await _conn.GetAsync<Person>(Guid.NewGuid());
        Assert.That(loaded, Is.Null);
    }

    [Test]
    public async Task GetListPagedAsync_WithOrderByLastName_ReturnsExpectedPages()
    {
        // Arrange
        await SeedManyAsync(new[]
        {
            ("Ada",   "Aardvark", 30),
            ("Bob",   "Brown",    40),
            ("Carl",  "Clark",    50),
            ("Diana", "Doe",      60),
            ("Evan",  "Evans",    70),
        });

        // Act
        var page1 = (await _conn.GetListPagedAsync<Person>(pageNumber: 1, rowsPerPage: 2,
            conditions: "", orderBy: "LastName ASC")).ToList();
        var page2 = (await _conn.GetListPagedAsync<Person>(pageNumber: 2, rowsPerPage: 2,
            conditions: "", orderBy: "LastName ASC")).ToList();
        var page3 = (await _conn.GetListPagedAsync<Person>(pageNumber: 3, rowsPerPage: 2,
            conditions: "", orderBy: "LastName ASC")).ToList();

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(page1.Select(x => x.LastName), Is.EqualTo(new[] { "Aardvark", "Brown" }));
            Assert.That(page2.Select(x => x.LastName), Is.EqualTo(new[] { "Clark", "Doe" }));
            Assert.That(page3.Select(x => x.LastName), Is.EqualTo(new[] { "Evans" }));
        });
    }

    [Test]
    public async Task GetListPagedAsync_WithNullOrderBy_FallsBackToPrimaryKey()
    {
        // Arrange
        var seededIds = (await SeedManyAsync(new[]
        {
            ("Ada",  "Zeta", 30),
            ("Bob",  "Yule", 40),
            ("Carl", "Xeno", 50),
        })).ToList();

        // Expected: fallback ORDER BY PK ASC; SQLite stores GUID as TEXT → lexicographic
        var expected = seededIds
            .Select(id => id.ToString())
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        // Act
        var page = (await _conn.GetListPagedAsync<Person>(
            pageNumber: 1,
            rowsPerPage: 3,
            conditions: "",
            orderBy: null
        )).ToList();

        var actual = page.Select(p => p.Id.ToString()).ToList();

        // Assert
        Assert.That(actual, Is.EqualTo(expected));
    }

    [Test]
    public void GetListPagedAsync_WithInvalidOrderByColumn_Throws()
    {
        Assert.That(
            async () => await _conn.GetListPagedAsync<Person>(1, 2, "", "Nope DESC"),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("Invalid ORDER BY column"));
    }

    [Test]
    public void GetListPagedAsync_WithInvalidOrderByDirection_Throws()
    {
        Assert.That(
            async () => await _conn.GetListPagedAsync<Person>(1, 2, "", "LastName SIDEWAYS"),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("Invalid ORDER BY direction"));
    }

    [Test]
    public async Task UpdateAsync_WithModifiedEntity_PersistsChanges()
    {
        var p = await SeedAsync("Barbara", "Liskov", 55);
        p.Age = 56;

        var rows = await _conn.UpdateAsync(p);

        Assert.That(rows, Is.EqualTo(1));
        Assert.That((await _conn.GetAsync<Person>(p.Id))!.Age, Is.EqualTo(56));
    }

    [Test]
    public async Task DeleteAsync_ById_RemovesRow()
    {
        var p = await SeedAsync("Grace", "Hopper", 70);

        var rows = await _conn.DeleteAsync<Person>(p.Id);

        Assert.That(rows, Is.EqualTo(1));
        Assert.That(await _conn.GetAsync<Person>(p.Id), Is.Null);
    }

    [Test]
    public async Task DeleteAsync_ByEntity_RemovesRow()
    {
        var p = await SeedAsync("Edsger", "Dijkstra", 55);

        var rows = await _conn.DeleteAsync(p);

        Assert.That(rows, Is.EqualTo(1));
        Assert.That(await _conn.GetAsync<Person>(p.Id), Is.Null);
    }

    [Test]
    public async Task GetListAsync_WithAnonymousFilter_ReturnsFilteredResults()
    {
        await SeedAsync("Alan", "Turing", 41);

        var results = (await _conn.GetListAsync<Person>(new { LastName = "Turing" })).ToList();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].FirstName, Is.EqualTo("Alan"));
    }

    [Test]
    public async Task GetListAsync_WithTypedOrderBy_ReturnsSortedAscendingThenDescending()
    {
        await SeedAsync("Edsger", "Dijkstra", 55);
        await SeedAsync("Alan", "Turing", 41);

        var results = (await _conn.GetListAsync<Person>(
            whereConditions: null,
            orderBy: new[]
            {
                ConnectionExtensions.OrderByAscending<Person>(x => x.LastName),
                ConnectionExtensions.OrderByDescending<Person>(x => x.Age)
            })).ToList();

        Assert.That(results.First().LastName, Is.EqualTo("Dijkstra"));
        Assert.That(results.Last().LastName,  Is.EqualTo("Turing"));
    }

    [Test]
    public async Task GetListAsync_WithValidRawOrderBy_ReturnsSortedResults()
    {
        await SeedAsync("Edsger", "Dijkstra", 55);
        await SeedAsync("Alan", "Turing", 41);

        var ok = (await _conn.GetListAsync<Person>(conditions: "", parameters: null,
                                                   orderBy: "LastName asc, Age DESC")).ToList();
        Assert.That(ok, Is.Not.Empty);
    }

    [Test]
    public void GetListAsync_WithInvalidColumnInOrderBy_ThrowsArgumentException()
    {
        Assert.That(async () =>
            (await _conn.GetListAsync<Person>("", null, "Nope DESC")).ToList(),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("Invalid ORDER BY column"));
    }

    [Test]
    public void GetListAsync_WithInvalidDirectionInOrderBy_ThrowsArgumentException()
    {
        Assert.That(async () =>
            (await _conn.GetListAsync<Person>("", null, "LastName SIDEWAYS")).ToList(),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("Invalid ORDER BY direction"));
    }

    [Test]
    public async Task RecordCountAsync_WithoutFilter_ReturnsTotalRowCount()
    {
        await SeedAsync("Alan", "Turing", 41);

        var total = await _conn.RecordCountAsync<Person>();
        Assert.That(total, Is.GreaterThan(0));
    }

    [Test]
    public async Task DeleteListAsync_WithAnonymousFilter_RemovesMatchingRows()
    {
        var p = await SeedAsync("Bob", "Dylan", 70);

        var removed = await _conn.DeleteListAsync<Person>(new { LastName = p.LastName });

        Assert.That(removed, Is.EqualTo(1));
        Assert.That(await _conn.RecordCountAsync<Person>("where LastName = @ln", new { ln = p.LastName }),
                    Is.EqualTo(0));
    }

    [Test]
    [Category("Integration")]
    public async Task RoundTripAsync_InsertGetUpdateDelete_ExecutesSuccessfully()
    {
        var p = new Person { Id = Guid.Empty, FirstName = "Barbara", LastName = "Liskov", Age = 55 };

        var key     = await _conn.InsertAsync<Guid, Person>(p);
        var loaded  = await _conn.GetAsync<Person>(key);
        loaded!.Age = 56;
        var updated = await _conn.UpdateAsync(loaded);
        var deleted = await _conn.DeleteAsync<Person>(key);

        Assert.Multiple(async () =>
        {
            Assert.That(key, Is.Not.EqualTo(Guid.Empty));
            Assert.That(updated, Is.EqualTo(1));
            Assert.That(deleted, Is.EqualTo(1));
            Assert.That(await _conn.GetAsync<Person>(key), Is.Null);
        });
    }

    // ---- helpers ----

    private async Task<Person> SeedAsync(string first, string last, int age)
    {
        var p = new Person { Id = Guid.Empty, FirstName = first, LastName = last, Age = age };
        var id = await _conn.InsertAsync<Guid, Person>(p);
        p.Id = id;
        return p;
    }

    private async Task<List<Guid>> SeedManyAsync((string first, string last, int age)[] items)
    {
        var ids = new List<Guid>(items.Length);
        foreach (var (first, last, age) in items)
        {
            var p = new Person { Id = Guid.Empty, FirstName = first, LastName = last, Age = age };
            var id = await _conn.InsertAsync<Guid, Person>(p);
            ids.Add(id);
        }
        return ids;
    }
}
