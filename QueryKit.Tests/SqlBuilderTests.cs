using System.Reflection;
using System.Text;
using QueryKit.Attributes;
using QueryKit.Dialects;
using QueryKit.Metadata;
using QueryKit.Sql;

namespace QueryKit.Tests;

[TestFixture]
public class SqlBuilderTests
{
    private SqlConvention _conv = null!;
    private SqlBuilder _builder = null!;

    [SetUp]
    public void SetUp()
    {
        var dialect = DialectConfig.Create(Dialect.SQLite); // "identifier"
        _conv = new SqlConvention(dialect, new TableNameResolver(), new ColumnNameResolver());
        _builder = new SqlBuilder(_conv);
    }

    // ----------------------------
    // BuildSelect
    // ----------------------------

    private sealed class SelectEntity
    {
        public Guid Id { get; set; }

        [Column("first_name")]
        public string FirstName { get; set; } = "";

        // should be skipped
        [IgnoreSelect]
        public string Secret { get; set; } = "";

        // should be skipped
        [NotMapped]
        public string NotInDb { get; set; } = "";
    }

    [Test]
    public void BuildSelect_EncapsulatesColumns_SkipsIgnoredAndNotMapped_AliasesWhenColumnAttributeAndDifferentName()
    {
        var props = SqlBuilder.GetScaffoldableProperties<SelectEntity>();
        var sb = new StringBuilder();

        _builder.BuildSelect(sb, props);

        // Expect: Id => "Id"
        // FirstName => raw column "first_name" => "first_name" AS "FirstName" (since CLR differs and ColumnAttribute exists)
        // Secret/NotInDb skipped
        Assert.That(sb.ToString(), Is.EqualTo("\"Id\", \"first_name\" AS \"FirstName\""));
    }

    private sealed class SelectNoAliasEntity
    {
        [Column("FirstName")] // same as CLR name (case-insensitive) => no alias
        public string FirstName { get; set; } = "";
    }

    [Test]
    public void BuildSelect_WhenColumnAttributeMatchesClrName_DoesNotAlias()
    {
        var props = SqlBuilder.GetScaffoldableProperties<SelectNoAliasEntity>();
        var sb = new StringBuilder();

        _builder.BuildSelect(sb, props);

        Assert.That(sb.ToString(), Is.EqualTo("\"FirstName\""));
    }

    private sealed class SelectAllSkipped
    {
        [IgnoreSelect] public string A { get; set; } = "";
        [NotMapped] public string B { get; set; } = "";
    }

    [Test]
    public void BuildSelect_WhenNoSelectableColumns_Throws()
    {
        var props = typeof(SelectAllSkipped).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var sb = new StringBuilder();

        Assert.That(
            () => _builder.BuildSelect(sb, props),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("No selectable columns"));
    }

    // ----------------------------
    // BuildWhere<TEntity>
    // ----------------------------

    private sealed class WhereEntity
    {
        public string LastName { get; set; } = "";

        [Column("age_years")]
        public int Age { get; set; }
    }

    [Test]
    public void BuildWhere_MapsAnonPropertiesToEntityProperties_CaseInsensitive_EncapsulatesColumns()
    {
        var where = new { lastname = "Turing", AGE = 41 }; // intentionally weird casing
        var props = where.GetType().GetProperties();
        var sb = new StringBuilder();

        _builder.BuildWhere<WhereEntity>(sb, props, where);

        // LastName => "LastName" = @LastName
        // Age => raw column "age_years" => "age_years" = @Age
        Assert.That(sb.ToString(), Is.EqualTo("\"LastName\" = @LastName AND \"age_years\" = @Age"));
    }

    [Test]
    public void BuildWhere_WhenValueIsNull_UsesIsNull()
    {
        var where = new { LastName = (string?)null };
        var props = where.GetType().GetProperties();
        var sb = new StringBuilder();

        _builder.BuildWhere<WhereEntity>(sb, props, where);

        Assert.That(sb.ToString(), Is.EqualTo("\"LastName\" IS NULL"));
    }

    [Test]
    public void BuildWhere_WhenValueIsDBNull_UsesIsNull()
    {
        var where = new { LastName = DBNull.Value };
        var props = where.GetType().GetProperties();
        var sb = new StringBuilder();

        _builder.BuildWhere<WhereEntity>(sb, props, where);

        Assert.That(sb.ToString(), Is.EqualTo("\"LastName\" IS NULL"));
    }

    [Test]
    public void BuildWhere_WhenAnonPropDoesNotExistOnEntity_Throws()
    {
        var where = new { Nope = 123 };
        var props = where.GetType().GetProperties();
        var sb = new StringBuilder();

        Assert.That(
            () => _builder.BuildWhere<WhereEntity>(sb, props, where),
            Throws.TypeOf<ArgumentException>().With.Message.Contains("does not exist"));
    }

    // ----------------------------
    // BuildInsertParameters / BuildInsertValues
    // ----------------------------

    private sealed class InsertEntity_IntId
    {
        public int Id { get; set; } // conventional int identity => should be skipped unless [Required]
        public string Name { get; set; } = "";
    }

    [Test]
    public void BuildInsertParameters_SkipsConventionalNonGuidId()
    {
        var sbCols = new StringBuilder();
        var sbVals = new StringBuilder();

        _builder.BuildInsertParameters<InsertEntity_IntId>(sbCols);
        _builder.BuildInsertValues<InsertEntity_IntId>(sbVals);

        Assert.That(sbCols.ToString(), Is.EqualTo("\"Name\""));
        Assert.That(sbVals.ToString(), Is.EqualTo("@Name"));
    }

    private sealed class InsertEntity_GuidId
    {
        public Guid Id { get; set; } // GUID id should be included
        public string Name { get; set; } = "";
    }

    [Test]
    public void BuildInsertParameters_IncludesGuidId()
    {
        var sbCols = new StringBuilder();
        var sbVals = new StringBuilder();

        _builder.BuildInsertParameters<InsertEntity_GuidId>(sbCols);
        _builder.BuildInsertValues<InsertEntity_GuidId>(sbVals);

        Assert.That(sbCols.ToString(), Is.EqualTo("\"Id\", \"Name\""));
        Assert.That(sbVals.ToString(), Is.EqualTo("@Id, @Name"));
    }

    private sealed class InsertEntity_IgnoreInsert
    {
        public Guid Id { get; set; }

        [IgnoreInsert]
        public string Name { get; set; } = "";
    }

    [Test]
    public void BuildInsertParameters_RespectsIgnoreInsert()
    {
        var sbCols = new StringBuilder();
        var sbVals = new StringBuilder();

        _builder.BuildInsertParameters<InsertEntity_IgnoreInsert>(sbCols);
        _builder.BuildInsertValues<InsertEntity_IgnoreInsert>(sbVals);

        Assert.That(sbCols.ToString(), Is.EqualTo("\"Id\""));
        Assert.That(sbVals.ToString(), Is.EqualTo("@Id"));
    }

    private sealed class InsertEntity_CompositeKey_GuidAndInt
    {
        [Key] public Guid OrderId { get; set; }
        [Key] public int LineNumber { get; set; }
        public string? Sku { get; set; }
    }

    [Test]
    public void BuildInsertParameters_IncludesAllCompositeKeyParts()
    {
        var sbCols = new StringBuilder();
        var sbVals = new StringBuilder();

        _builder.BuildInsertParameters<InsertEntity_CompositeKey_GuidAndInt>(sbCols);
        _builder.BuildInsertValues<InsertEntity_CompositeKey_GuidAndInt>(sbVals);

        Assert.That(sbCols.ToString(), Is.EqualTo("\"OrderId\", \"LineNumber\", \"Sku\""));
        Assert.That(sbVals.ToString(), Is.EqualTo("@OrderId, @LineNumber, @Sku"));
    }

    private sealed class InsertEntity_CompositeKey_TwoInts
    {
        [Key] public int TenantId { get; set; }
        [Key] public int RecordNumber { get; set; }
        public string? Name { get; set; }
    }

    [Test]
    public void BuildInsertParameters_CompositeIntKeys_IncludesBoth()
    {
        var sbCols = new StringBuilder();
        var sbVals = new StringBuilder();

        _builder.BuildInsertParameters<InsertEntity_CompositeKey_TwoInts>(sbCols);
        _builder.BuildInsertValues<InsertEntity_CompositeKey_TwoInts>(sbVals);

        Assert.That(sbCols.ToString(), Is.EqualTo("\"TenantId\", \"RecordNumber\", \"Name\""));
        Assert.That(sbVals.ToString(), Is.EqualTo("@TenantId, @RecordNumber, @Name"));
    }

    // ----------------------------
    // BuildUpdateSet
    // ----------------------------

    private sealed class UpdateEntity
    {
        public Guid Id { get; set; }

        public long Version { get; set; }

        public string Name { get; set; } = "";

        [IgnoreUpdate]
        public string DoNotUpdate { get; set; } = "";

        [ReadOnly]
        public string ReadOnlyThing { get; set; } = "";
    }

    [Test]
    public void BuildUpdateSet_UpdatesOnlyUpdateableProperties()
    {
        var e = new UpdateEntity
        {
            Id = Guid.NewGuid(),
            Version = 7,
            Name = "Updated",
            DoNotUpdate = "X",
            ReadOnlyThing = "Y"
        };

        var sb = new StringBuilder();
        _builder.BuildUpdateSet(e, sb);

        // only Name should appear; Id/Version excluded; IgnoreUpdate/ReadOnly excluded
        Assert.That(sb.ToString(), Is.EqualTo("\"Name\" = @Name"));
    }
}