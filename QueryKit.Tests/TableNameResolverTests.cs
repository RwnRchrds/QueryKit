using QueryKit.Attributes;
using QueryKit.Metadata;

namespace QueryKit.Tests;

[TestFixture]
public class TableNameResolverTests
{
    [Test]
    public void TableNameResolver_UsesQueryKitTableAttribute()
    {
        var r = new TableNameResolver();
        Assert.That(r.ResolveTableName(typeof(QkTableEntity)), Is.EqualTo("main.People"));
    }

    [Table("People", Schema = "main")]
    private sealed class QkTableEntity { }

    [Test]
    public void TableNameResolver_UsesDataAnnotationsTableAttribute()
    {
        var r = new TableNameResolver();
        Assert.That(r.ResolveTableName(typeof(DaTableEntity)), Is.EqualTo("dbo.People"));
    }

    [Table("People", Schema = "dbo")]
    private sealed class DaTableEntity { }

    [Test]
    public void TableNameResolver_WhenNoAttribute_ReturnsTypeName()
    {
        var r = new TableNameResolver();
        Assert.That(r.ResolveTableName(typeof(NoTableEntity)), Is.EqualTo("NoTableEntity"));
    }

    private sealed class NoTableEntity { }
}