using System;
using System.Linq;
using System.Reflection;
using NUnit.Framework;
using QueryKit.Attributes;
using QueryKit.Dialects;
using QueryKit.Metadata;
using QueryKit.Sql;

namespace QueryKit.Tests;

[TestFixture]
public class SqlConventionTests
{
    private SqlConvention _conv = null!;

    [SetUp]
    public void SetUp()
    {
        // Pick a dialect with a predictable encapsulation format
        var dialect = DialectConfig.Create(Dialect.SQLite);
        _conv = new SqlConvention(dialect, new TableNameResolver(), new ColumnNameResolver());
    }

    // -------------------------
    // Encapsulate
    // -------------------------

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    public void Encapsulate_WithNullOrWhitespace_Throws(string? input)
    {
        Assert.That(() => _conv.Encapsulate(input!), Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void Encapsulate_WithSingleToken_Encapsulates()
    {
        var x = _conv.Encapsulate("People");
        Assert.That(x, Is.EqualTo("\"People\"")); // SQLite dialect config is expected to use "..."
    }

    [Test]
    public void Encapsulate_WithMultipart_EncapsulatesEachPart()
    {
        var x = _conv.Encapsulate("main.People.Id");
        Assert.That(x, Is.EqualTo("\"main\".\"People\".\"Id\""));
    }

    [Test]
    public void Encapsulate_AnsiDialect_DoublesEmbeddedQuote()
    {
        // SQLite uses ANSI quoting: " -> ""
        var x = _conv.Encapsulate("Weird\"Name");
        Assert.That(x, Is.EqualTo("\"Weird\"\"Name\""));
    }

    [Test]
    public void Encapsulate_SqlServer_DoublesEmbeddedClosingBracket()
    {
        var conv = new SqlConvention(
            DialectConfig.Create(Dialect.SQLServer),
            new TableNameResolver(),
            new ColumnNameResolver());

        var x = conv.Encapsulate("Weird]Name");
        Assert.That(x, Is.EqualTo("[Weird]]Name]"));
    }

    [Test]
    public void Encapsulate_MySql_DoublesEmbeddedBacktick()
    {
        var conv = new SqlConvention(
            DialectConfig.Create(Dialect.MySQL),
            new TableNameResolver(),
            new ColumnNameResolver());

        var x = conv.Encapsulate("Weird`Name");
        Assert.That(x, Is.EqualTo("`Weird``Name`"));
    }

    [Test]
    public void DialectConfig_SqlServer_UsesOffsetFetchPaging()
    {
        var cfg = DialectConfig.Create(Dialect.SQLServer);
        Assert.That(cfg.PagedListSql, Does.Contain("OFFSET"));
        Assert.That(cfg.PagedListSql, Does.Contain("FETCH NEXT"));
        Assert.That(cfg.PagedListSql, Does.Not.Contain("PagedNumber"));
        Assert.That(cfg.PagedListSql, Does.Not.Contain("ROW_NUMBER"));
    }

    // -------------------------
    // GetIdProperties
    // -------------------------

    private sealed class HasConventionalId
    {
        public Guid Id { get; set; }
    }

    private sealed class HasKeyAttribute
    {
        [Key]
        public int PersonId { get; set; }

        public int Id { get; set; } // should be ignored because [Key] exists
    }

    [Test]
    public void GetIdProperties_WhenNoKeyAttribute_FallsBackToIdProperty()
    {
        var props = SqlConvention.GetIdProperties(typeof(HasConventionalId));
        Assert.That(props.Select(p => p.Name), Is.EquivalentTo(new[] { "Id" }));
    }

    [Test]
    public void GetIdProperties_WhenKeyAttributePresent_UsesKeyAttribute()
    {
        var props = SqlConvention.GetIdProperties(typeof(HasKeyAttribute));
        Assert.That(props.Select(p => p.Name), Is.EquivalentTo(new[] { "PersonId" }));
    }

    private sealed class HasDataAnnotationsKey
    {
        [System.ComponentModel.DataAnnotations.Key]
        public int PersonId { get; set; }

        public int Id { get; set; }
    }

    [Test]
    public void GetIdProperties_AlsoRecognizesForeignKeyAttribute()
    {
        var props = SqlConvention.GetIdProperties(typeof(HasDataAnnotationsKey));
        Assert.That(props.Select(p => p.Name), Is.EquivalentTo(new[] { "PersonId" }));
    }

    private sealed class HasDataAnnotationsColumn
    {
        [System.ComponentModel.DataAnnotations.Schema.Column("first_name")]
        public string FirstName { get; set; } = "";
    }

    [Test]
    public void GetColumnName_AlsoRecognizesForeignColumnAttribute()
    {
        var pi = typeof(HasDataAnnotationsColumn).GetProperty(nameof(HasDataAnnotationsColumn.FirstName))!;
        Assert.That(_conv.GetColumnName(pi), Is.EqualTo("first_name"));
    }

    // -------------------------
    // Version property detection
    // -------------------------

    private sealed class NoVersionHere
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class ConventionalVersion
    {
        public Guid Id { get; set; }
        public long Version { get; set; }
    }

    private sealed class AttributedVersion
    {
        public Guid Id { get; set; }

        [Version]
        public long Revision { get; set; }
    }

    private sealed class WrongTypeVersion
    {
        public Guid Id { get; set; }
        public int Version { get; set; } // wrong
    }

    private sealed class NullableVersion
    {
        public Guid Id { get; set; }

        [Version]
        public long? Revision { get; set; } // wrong (nullable)
    }

    private sealed class TwoAttributedVersions
    {
        public Guid Id { get; set; }

        [Version] public long RevA { get; set; }
        [Version] public long RevB { get; set; }
    }

    [Test]
    public void GetVersionProperty_WhenNoneDefined_ReturnsNull()
    {
        var prop = SqlConvention.GetVersionProperty(typeof(NoVersionHere));
        Assert.That(prop, Is.Null);
    }

    [Test]
    public void TryGetVersionProperty_WhenNoneDefined_ReturnsFalseAndNull()
    {
        var ok = SqlConvention.TryGetVersionProperty(typeof(NoVersionHere), out var prop);
        Assert.That(ok, Is.False);
        Assert.That(prop, Is.Null);
    }

    [Test]
    public void GetVersionProperty_WhenConventionalLongVersion_ReturnsProperty()
    {
        var prop = SqlConvention.GetVersionProperty(typeof(ConventionalVersion));
        Assert.That(prop, Is.Not.Null);
        Assert.That(prop!.Name, Is.EqualTo("Version"));
        Assert.That(prop.PropertyType, Is.EqualTo(typeof(long)));
    }

    [Test]
    public void GetVersionProperty_WhenAttributedLongVersion_ReturnsAttributedProperty()
    {
        var prop = SqlConvention.GetVersionProperty(typeof(AttributedVersion));
        Assert.That(prop, Is.Not.Null);
        Assert.That(prop!.Name, Is.EqualTo("Revision"));
        Assert.That(prop.PropertyType, Is.EqualTo(typeof(long)));
    }

    [Test]
    public void GetVersionProperty_WhenWrongType_Throws()
    {
        Assert.That(
            () => SqlConvention.GetVersionProperty(typeof(WrongTypeVersion)),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("must be of type long"));
    }

    [Test]
    public void GetVersionProperty_WhenNullableLong_Throws()
    {
        Assert.That(
            () => SqlConvention.GetVersionProperty(typeof(NullableVersion)),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("must be of type long"));
    }

    [Test]
    public void GetVersionProperty_WhenMultipleVersionAttributes_Throws()
    {
        Assert.That(
            () => SqlConvention.GetVersionProperty(typeof(TwoAttributedVersions)),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("multiple properties"));
    }

    [Test]
    public void TryGetVersionProperty_WhenMisconfigured_PropagatesArgumentException()
    {
        // TryGetVersionProperty must NOT swallow config errors
        Assert.That(
            () => SqlConvention.TryGetVersionProperty(typeof(TwoAttributedVersions), out _),
            Throws.TypeOf<ArgumentException>());
    }

    [Test]
    public void SequentialGuid_MaxDate_UsesUShortRange_To2079()
    {
        var baseDate = new DateTime(1900, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var max = baseDate.AddDays(ushort.MaxValue);

        Assert.That(max, Is.EqualTo(new DateTime(2079, 6, 6, 0, 0, 0, DateTimeKind.Utc)));
    }

    [Test]
    public void SequentialGuid_GeneratesNonEmptyUniqueValues()
    {
        var set = new HashSet<Guid>();

        for (int i = 0; i < 1000; i++)
        {
            var g = SqlConvention.SequentialGuid();
            Assert.That(g, Is.Not.EqualTo(Guid.Empty));
            Assert.That(set.Add(g), Is.True, "Duplicate GUID generated");
        }
    }

    [Test]
    public void SequentialGuid_ConcurrentCalls_ProduceUniqueTimestampPortions()
    {
        const int callsPerThread = 500;
        const int threadCount = 8;
        var bag = new System.Collections.Concurrent.ConcurrentBag<Guid>();

        System.Threading.Tasks.Parallel.For(0, threadCount, _ =>
        {
            for (int i = 0; i < callsPerThread; i++)
                bag.Add(SqlConvention.SequentialGuid());
        });

        // Last 6 bytes of ToByteArray() are the SQL-Server-style sortable timestamp (big-endian).
        var timestamps = bag.Select(g =>
        {
            var b = g.ToByteArray();
            long t = 0;
            for (int i = 10; i < 16; i++) t = (t << 8) | b[i];
            return t;
        }).ToArray();

        Assert.That(timestamps.Distinct().Count(), Is.EqualTo(timestamps.Length),
            "Concurrent SequentialGuid calls must produce unique timestamp portions.");
    }

    // -------------------------
    // Column name resolution integration (minimal)
    // -------------------------

    private sealed class ColumnNameEntity
    {
        [Column("first_name")]
        public string FirstName { get; set; } = "";
    }

    [Test]
    public void GetColumnName_WhenColumnAttributePresent_UsesAttributeName()
    {
        var pi = typeof(ColumnNameEntity).GetProperty(nameof(ColumnNameEntity.FirstName))!;
        var raw = _conv.GetColumnName(pi);
        Assert.That(raw, Is.EqualTo("first_name"));

        var encapsulated = _conv.GetColumnNameEncapsulated(pi);
        Assert.That(encapsulated, Is.EqualTo("\"first_name\""));
    }
}