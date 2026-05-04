using System.Data;
using Dapper;
using QueryKit.Dialects;
using QueryKit.Extensions;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

[TestFixture]
public class CompositeKeyTests
{
    private IDbConnection _conn = null!;
    
    [SetUp]
    public void SetUp()
    {
        _conn = TestDatabase.OpenAndInit();
        ConnectionExtensions.UseDialect(Dialect.SQLite);

        // Create a dedicated table for composite-key testing
        _conn.Execute(@"
            create table if not exists OrderItems (
                OrderId   TEXT    not null,
                LineNumber INTEGER not null,
                Sku       TEXT,
                primary key (OrderId, LineNumber)
            );");
    }
    
    [TearDown]
    public void TearDown()
    {
        // Clean up this test-only table
        _conn.Execute(@"drop table if exists OrderItems;");
        _conn.Dispose();
    }
    
     [Test]
    public void Delete_ByCompositeId_RemovesRow_Sync()
    {
        // Arrange
        var oid = Guid.NewGuid();
        _conn.Execute(
            "insert into OrderItems (OrderId, LineNumber, Sku) values (@OrderId, @LineNumber, @Sku)",
            new { OrderId = oid, LineNumber = 1, Sku = "SYNC-ROW" });

        // Sanity check present
        var before = _conn.ExecuteScalar<int>(
            "select count(*) from OrderItems where OrderId=@OrderId and LineNumber=@LineNumber",
            new { OrderId = oid, LineNumber = 1 });
        Assert.That(before, Is.EqualTo(1));

        // Act: delete using the composite-id overload
        var affected = _conn.Delete<OrderItem>(new { OrderId = oid, LineNumber = 1 });

        // Assert
        Assert.That(affected, Is.EqualTo(1));
        var after = _conn.ExecuteScalar<int>(
            "select count(*) from OrderItems where OrderId=@OrderId and LineNumber=@LineNumber",
            new { OrderId = oid, LineNumber = 1 });
        Assert.That(after, Is.EqualTo(0));
    }

    [Test]
    public void Insert_CompositeKeyEntity_IncludesAllKeyParts_Sync()
    {
        var oid = Guid.NewGuid();
        var item = new OrderItem { OrderId = oid, LineNumber = 7, Sku = "INS-SYNC" };

        _conn.Insert<OrderItem>(item);

        var count = _conn.ExecuteScalar<int>(
            "select count(*) from OrderItems where OrderId=@OrderId and LineNumber=@LineNumber and Sku=@Sku",
            new { OrderId = oid, LineNumber = 7, Sku = "INS-SYNC" });
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task Insert_CompositeKeyEntity_IncludesAllKeyParts_Async()
    {
        var oid = Guid.NewGuid();
        var item = new OrderItem { OrderId = oid, LineNumber = 9, Sku = "INS-ASYNC" };

        await _conn.InsertAsync<OrderItem>(item);

        var count = await _conn.ExecuteScalarAsync<int>(
            "select count(*) from OrderItems where OrderId=@OrderId and LineNumber=@LineNumber and Sku=@Sku",
            new { OrderId = oid, LineNumber = 9, Sku = "INS-ASYNC" });
        Assert.That(count, Is.EqualTo(1));
    }

    [Test]
    public async Task Delete_ByCompositeId_RemovesRow_Async()
    {
        // Arrange
        var oid = Guid.NewGuid();
        await _conn.ExecuteAsync(
            "insert into OrderItems (OrderId, LineNumber, Sku) values (@OrderId, @LineNumber, @Sku)",
            new { OrderId = oid, LineNumber = 2, Sku = "ASYNC-ROW" });

        var before = await _conn.ExecuteScalarAsync<int>(
            "select count(*) from OrderItems where OrderId=@OrderId and LineNumber=@LineNumber",
            new { OrderId = oid, LineNumber = 2 });
        Assert.That(before, Is.EqualTo(1));

        // Act: delete using the composite-id overload
        var affected = await _conn.DeleteAsync<OrderItem>(new { OrderId = oid, LineNumber = 2 });

        // Assert
        Assert.That(affected, Is.EqualTo(1));
        var after = await _conn.ExecuteScalarAsync<int>(
            "select count(*) from OrderItems where OrderId=@OrderId and LineNumber=@LineNumber",
            new { OrderId = oid, LineNumber = 2 });
        Assert.That(after, Is.EqualTo(0));
    }
}