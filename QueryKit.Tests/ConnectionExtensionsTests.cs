using System.Data;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

[TestFixture]
public class ConnectionExtensionsTests
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
    public void Insert_WithNewEntity_GeneratesGuidKeyAndSetsIdOnEntity()
    {
        var p = new Person { Id = Guid.Empty, FirstName = "Barbara", LastName = "Liskov", Age = 55 };

        var key = _conn.Insert<Guid, Person>(p);

        Assert.That(key, Is.Not.EqualTo(Guid.Empty));
        Assert.That(p.Id, Is.EqualTo(key));
    }

    [Test]
    public void Get_WithExistingId_ReturnsMatchingEntity()
    {
        var p = Seed("Barbara", "Liskov", 55);

        var loaded = _conn.Get<Person>(p.Id);

        Assert.That(loaded, Is.Not.Null);
        Assert.That(loaded!.FirstName, Is.EqualTo("Barbara"));
        Assert.That(loaded.LastName,  Is.EqualTo("Liskov"));
        Assert.That(loaded.Age,       Is.EqualTo(55));
    }

    [Test]
    public void Get_WithMissingId_ReturnsNull()
    {
        var loaded = _conn.Get<Person>(Guid.NewGuid());
        Assert.That(loaded, Is.Null);
    }
    
    [Test]
    public void GetListPaged_WithOrderByLastName_ReturnsExpectedPages()
    {
        // Arrange: deterministic set
        var ids = SeedMany(new[]
        {
            ("Ada",   "Aardvark", 30),
            ("Bob",   "Brown",    40),
            ("Carl",  "Clark",    50),
            ("Diana", "Doe",      60),
            ("Evan",  "Evans",    70),
        });

        // Act
        var page1 = _conn.GetListPaged<Person>(pageNumber: 1, rowsPerPage: 2,
            conditions: "", orderBy: "LastName ASC").ToList();
        var page2 = _conn.GetListPaged<Person>(pageNumber: 2, rowsPerPage: 2,
            conditions: "", orderBy: "LastName ASC").ToList();
        var page3 = _conn.GetListPaged<Person>(pageNumber: 3, rowsPerPage: 2,
            conditions: "", orderBy: "LastName ASC").ToList();

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(page1.Select(x => x.LastName), Is.EqualTo(new[] { "Aardvark", "Brown" }));
            Assert.That(page2.Select(x => x.LastName), Is.EqualTo(new[] { "Clark", "Doe" }));
            Assert.That(page3.Select(x => x.LastName), Is.EqualTo(new[] { "Evans" }));
        });
    }
    
    [Test]
    public void GetListPaged_WithNullOrderBy_FallsBackToPrimaryKey()
    {
        // Arrange
        var seededIds = SeedMany(new[]
        {
            ("Ada",  "Zeta", 30),
            ("Bob",  "Yule", 40),
            ("Carl", "Xeno", 50),
        }).ToList(); // materialize to avoid lazy enumerables in diffs

        // Expected: when orderBy is null, we fall back to ORDER BY PK ASC.
        // In our SQLite test DB, GUIDs are stored as TEXT, so ASC is lexicographic.
        var expected = seededIds
            .Select(id => id.ToString())
            .OrderBy(s => s, StringComparer.Ordinal)
            .ToList();

        // Act
        var page = _conn.GetListPaged<Person>(
            pageNumber: 1,
            rowsPerPage: 3,
            conditions: "",
            orderBy: null
        ).ToList();

        var actual = page.Select(p => p.Id.ToString()).ToList();

        // Assert
        Assert.That(actual, Is.EqualTo(expected));
    }
    
    [Test]
    public void GetListPaged_WithInvalidOrderByColumn_Throws()
    {
        Assert.That(
            () => _conn.GetListPaged<Person>(1, 2, "", "Nope DESC").ToList(),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("Invalid ORDER BY column"));
    }

    [Test]
    public void GetListPaged_WithInvalidOrderByDirection_Throws()
    {
        Assert.That(
            () => _conn.GetListPaged<Person>(1, 2, "", "LastName SIDEWAYS").ToList(),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("Invalid ORDER BY direction"));
    }

    [Test]
    public void Update_WithModifiedEntity_PersistsChanges()
    {
        var p = Seed("Barbara", "Liskov", 55);
        p.Age = 56;

        var rows = _conn.Update(p);

        Assert.That(rows, Is.EqualTo(1));
        Assert.That(_conn.Get<Person>(p.Id)!.Age, Is.EqualTo(56));
    }

    [Test]
    public void Delete_ById_RemovesRow()
    {
        var p = Seed("Grace", "Hopper", 70);

        var rows = _conn.Delete<Person>(p.Id);

        Assert.That(rows, Is.EqualTo(1));
        Assert.That(_conn.Get<Person>(p.Id), Is.Null);
    }

    [Test]
    public void Delete_ByEntity_RemovesRow()
    {
        var p = Seed("Edsger", "Dijkstra", 55);

        var rows = _conn.Delete(p);

        Assert.That(rows, Is.EqualTo(1));
        Assert.That(_conn.Get<Person>(p.Id), Is.Null);
    }

    [Test]
    public void GetList_WithAnonymousFilter_ReturnsFilteredResults()
    {
        TestDatabase.Seed(_conn);
        
        var results = _conn.GetList<Person>(new { LastName = "Turing" }).ToList();

        Assert.That(results, Has.Count.EqualTo(1));
        Assert.That(results[0].FirstName, Is.EqualTo("Alan"));
    }

    [Test]
    public void GetList_WithTypedOrderBy_ReturnsSortedAscendingThenDescending()
    {
        TestDatabase.Seed(_conn);
        
        var results = _conn.GetList<Person>(
            whereConditions: null,
            orderBy:
            [
                ConnectionExtensions.OrderByAscending<Person>(x => x.LastName),
                ConnectionExtensions.OrderByDescending<Person>(x => x.Age)
            ]).ToList();

        Assert.That(results.First().LastName, Is.EqualTo("Dijkstra"));
        Assert.That(results.Last().LastName,  Is.EqualTo("Turing"));
    }

    [Test]
    public void GetList_WithValidRawOrderBy_ReturnsSortedResults()
    {
        TestDatabase.Seed(_conn);
        
        var ok = _conn.GetList<Person>(conditions: "", parameters: null,
                                       orderBy: "LastName asc, Age DESC").ToList();
        Assert.That(ok, Is.Not.Empty);
    }

    [Test]
    public void GetList_WithInvalidColumnInOrderBy_ThrowsArgumentException()
    {
        TestDelegate act = () => _conn.GetList<Person>("", null, "Nope DESC").ToList();
        Assert.That(act, Throws.TypeOf<ArgumentException>()
                               .With.Message.Contains("Invalid ORDER BY column"));
    }

    [Test]
    public void GetList_WithInvalidDirectionInOrderBy_ThrowsArgumentException()
    {
        TestDelegate act = () => _conn.GetList<Person>("", null, "LastName SIDEWAYS").ToList();
        Assert.That(act, Throws.TypeOf<ArgumentException>()
                               .With.Message.Contains("Invalid ORDER BY direction"));
    }

    [Test]
    public void RecordCount_WithoutFilter_ReturnsTotalRowCount()
    {
        TestDatabase.Seed(_conn);
        
        var total = _conn.RecordCount<Person>();
        Assert.That(total, Is.GreaterThan(0));
    }

    [Test]
    public void DeleteList_WithAnonymousFilter_RemovesMatchingRows()
    {
        var p = Seed("Bob", "Dylan", 70);

        var removed = _conn.DeleteList<Person>(new { LastName = p.LastName });

        Assert.That(removed, Is.EqualTo(1));
        Assert.That(_conn.RecordCount<Person>("where LastName = @ln", new { ln = p.LastName }),
                    Is.EqualTo(0));
    }

    [Test]
    [Category("Integration")]
    public void RoundTrip_InsertGetUpdateDelete_ExecutesSuccessfully()
    {
        var p = new Person { Id = Guid.Empty, FirstName = "Barbara", LastName = "Liskov", Age = 55 };

        var key     = _conn.Insert<Guid, Person>(p);
        var loaded  = _conn.Get<Person>(key);
        loaded!.Age = 56;
        var updated = _conn.Update(loaded);
        var deleted = _conn.Delete<Person>(key);

        Assert.Multiple(() =>
        {
            Assert.That(key, Is.Not.EqualTo(Guid.Empty));
            Assert.That(updated, Is.EqualTo(1));
            Assert.That(deleted, Is.EqualTo(1));
            Assert.That(_conn.Get<Person>(key), Is.Null);
        });
    }

    private Person Seed(string first, string last, int age)
    {
        var p = new Person { Id = Guid.Empty, FirstName = first, LastName = last, Age = age };
        var id = _conn.Insert<Guid, Person>(p);
        p.Id = id;
        return p;
    }
    
    private List<Guid> SeedMany((string first, string last, int age)[] items)
    {
        var ids = new List<Guid>(items.Length);
        foreach (var (first, last, age) in items)
        {
            var p = new Person { Id = Guid.Empty, FirstName = first, LastName = last, Age = age };
            var id = _conn.Insert<Guid, Person>(p);
            ids.Add(id);
        }
        return ids;
    }
}
