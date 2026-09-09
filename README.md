# QueryKit

> Lightweight, dependency-light SQL builder + CRUD extensions for Dapper.
> Dialect-aware. Attribute-friendly. Zero ceremony.

- **NuGet**: `BlockSoftware.QueryKit`
- **Targets**: `net8.0` (recommended) + `netstandard2.0`
- **Databases**: SQL Server, PostgreSQL, MySQL/MariaDB, SQLite, Oracle, Db2

---

## Table of contents

- [Why QueryKit?](#why-querykit)
- [Install](#install)
- [Quick start](#quick-start)
- [Mapping entities with attributes](#mapping-entities-with-attributes)
- [Reading data](#reading-data)
  - [Get a single row by key](#get-a-single-row-by-key)
  - [GetList with anonymous filters](#getlist-with-anonymous-filters)
  - [GetList with a raw WHERE fragment](#getlist-with-a-raw-where-fragment)
  - [GetList for all rows](#getlist-for-all-rows)
  - [GetListPaged](#getlistpaged)
  - [RecordCount](#recordcount)
- [Writing data](#writing-data)
  - [Insert](#insert)
  - [BatchInsert](#batchinsert)
  - [Update](#update)
  - [Optimistic concurrency with UpdateWithVersion](#optimistic-concurrency-with-updatewithversion)
  - [Delete](#delete)
  - [DeleteList](#deletelist)
- [Stored procedures](#stored-procedures)
- [Async API](#async-api)
- [Choosing a dialect](#choosing-a-dialect)
- [Composite keys](#composite-keys)
- [Transactions](#transactions)
- [Logging generated SQL](#logging-generated-sql)
- [Safety notes](#safety-notes)

---

## Why QueryKit?

- **Tiny surface** — extension methods on `IDbConnection` (sync + async).
- **Predictable SQL** — no LINQ provider, no expression trees pretending to be queries; you can log every statement.
- **Dialect-aware** — identifier quoting, identity retrieval, and paging are picked per database.
- **Attribute-friendly** — supports `[Key]`, `[Column]`, `[Table]`, `[NotMapped]`, `[IgnoreInsert]`, `[IgnoreUpdate]`, `[IgnoreSelect]`, `[IgnoreCrud]`, `[ReadOnly]`, `[Required]`, `[Version]`, `[Scaffold]`.
- **Batteries included** — paging, composite keys, anonymous filters, raw `WHERE` fragments, optimistic concurrency, and stored-procedure execution.

---

## Install

```bash
dotnet add package BlockSoftware.QueryKit
```

> Previously published as `Rowan.QueryKit`. The package ID changed at 0.10.0; the namespaces,
> types and API are unchanged, so switching is a one-line edit to your `PackageReference`.

QueryKit depends on `Dapper`. Bring your own `IDbConnection` (e.g. `SqlConnection`, `NpgsqlConnection`, `MySqlConnection`, `SqliteConnection`, `OracleConnection`).

---

## Quick start

```csharp
using System.Data;
using Microsoft.Data.SqlClient;
using QueryKit.Attributes;
using QueryKit.Dialects;
using QueryKit.Extensions;

// Once at startup — defaults to SQL Server if you skip this.
ConnectionExtensions.UseDialect(Dialect.SQLServer);

[Table("Users", "dbo")]
public class User
{
    [Key]
    public int Id { get; set; }

    [Column("first_name")]
    public string FirstName { get; set; } = "";

    [Column("last_name")]
    public string LastName { get; set; } = "";

    public DateTime CreatedUtc { get; set; }

    [NotMapped]
    public string FullName => $"{FirstName} {LastName}";
}

using IDbConnection db = new SqlConnection(connectionString);
db.Open();

var newId = db.Insert<int, User>(new User
{
    FirstName = "Ada",
    LastName  = "Lovelace",
    CreatedUtc = DateTime.UtcNow
});

var ada    = db.Get<User>(newId);
var lovela = db.GetList<User>(new { LastName = "Lovelace" });
var page1  = db.GetListPaged<User>(pageNumber: 1, rowsPerPage: 25,
                                   conditions: "where CreatedUtc > @since",
                                   orderBy:   "CreatedUtc DESC",
                                   parameters: new { since = DateTime.UtcNow.AddDays(-7) });

ada!.LastName = "King-Lovelace";
db.Update(ada);

db.Delete<User>(ada.Id);
```

---

## Mapping entities with attributes

| Attribute         | Applies to | Effect                                                                                         |
| ----------------- | ---------- | ---------------------------------------------------------------------------------------------- |
| `[Table(name, schema?)]` | class | Overrides the table name (and optional schema). Defaults to the class name.                  |
| `[Column("col")]` | property   | Overrides the column name. Defaults to the property name.                                      |
| `[Key]`           | property   | Marks one or more primary-key properties. Without it, a property called `Id` is used.          |
| `[Required]`      | property   | Required on insert (used when the key is a string).                                            |
| `[NotMapped]`     | property   | Property is skipped for `SELECT`, `INSERT`, and `UPDATE`.                                      |
| `[IgnoreCrud]`    | property   | Same as `[NotMapped]` — skipped everywhere.                                                    |
| `[IgnoreSelect]`  | property   | Skipped from generated `SELECT` lists.                                                         |
| `[IgnoreInsert]`  | property   | Skipped from generated `INSERT` statements (e.g. computed columns).                            |
| `[IgnoreUpdate]`  | property   | Skipped from generated `UPDATE` statements.                                                    |
| `[ReadOnly]`      | property   | Inserted and selected, but never updated.                                                      |
| `[Version]`       | property   | Marks a `long` optimistic-concurrency column. Property may also simply be named `Version`.     |
| `[Scaffold]`      | property   | Treats a property as a column even though its type is not a simple type. Without it the property is left out of `SELECT`, `INSERT` **and** `UPDATE`, so a value set on it is silently dropped. Needed for `TimeOnly` / `DateOnly`. |

### Example

```csharp
[Table("Orders")]
public class Order
{
    [Key]
    public Guid Id { get; set; }                 // auto-filled with a sequential GUID on insert

    [Column("order_no")]
    public string OrderNumber { get; set; } = "";

    [ReadOnly]
    public DateTime CreatedUtc { get; set; }     // selected and inserted, never updated

    [IgnoreInsert]
    public DateTime? UpdatedUtc { get; set; }    // populated by a trigger or default

    [IgnoreSelect]
    public string? InternalNotes { get; set; }   // written but never read back

    [NotMapped]
    public bool IsRecent => CreatedUtc > DateTime.UtcNow.AddDays(-1);

    [Version]
    public long Revision { get; set; }
}
```

---

## Reading data

### Get a single row by key

```csharp
var user = db.Get<User>(42);
// SELECT [Id], [first_name], [last_name], [CreatedUtc] FROM [dbo].[Users] WHERE [Id] = @Id
```

For composite keys, pass an object that exposes each key property:

```csharp
var line = db.Get<OrderLine>(new { OrderId = orderId, LineNo = 3 });
```

### GetList with anonymous filters

Equality filters built from an anonymous object are the simplest way to query:

```csharp
var active = db.GetList<User>(new { IsActive = true });

// With strongly-typed ORDER BY
var sorted = db.GetList<User>(
    new { IsActive = true },
    orderBy: new[]
    {
        ConnectionExtensions.OrderByDescending<User>(u => u.CreatedUtc),
        ConnectionExtensions.OrderByAscending<User>(u => u.LastName)
    });
```

### GetList with a raw WHERE fragment

When you need anything beyond equality, pass a SQL fragment plus parameters:

```csharp
var recent = db.GetList<User>(
    conditions: "where CreatedUtc >= @since and LastName like @name",
    parameters: new { since = DateTime.UtcNow.AddMonths(-1), name = "Lov%" },
    orderBy: "LastName ASC, FirstName ASC");
```

The `orderBy` string is **validated against an allowlist** of mapped columns/property names per entity, so you can pass user-supplied sort tokens without opening up SQL injection. Quoted forms (`[col]`, `"col"`, `` `col` ``) and `Schema.Column` are accepted.

### GetList for all rows

```csharp
var everyone = db.GetList<User>();
```

### GetListPaged

Dialect-aware pagination. Pages are 1-indexed.

```csharp
var page = db.GetListPaged<User>(
    pageNumber: 2,
    rowsPerPage: 50,
    conditions: "where IsActive = @active",
    orderBy: "CreatedUtc DESC, Id ASC",
    parameters: new { active = true });
```

If you omit `orderBy`, QueryKit uses the first `[Key]` property. Throws `NotSupportedException` for dialects that have no paging template configured.

### RecordCount

```csharp
var total       = db.RecordCount<User>();
var activeCount = db.RecordCount<User>("where IsActive = @active", new { active = true });
```

---

## Writing data

### Insert

```csharp
// Returns the generated key as object (for callers that don't care about its type)
db.Insert(new User { FirstName = "Ada", LastName = "Lovelace" });

// Strongly-typed key
int id = db.Insert<int, User>(new User { FirstName = "Ada", LastName = "Lovelace" })!;

// Guid key — QueryKit fills in a monotonically increasing sequential GUID if Id is empty
var orderId = db.Insert<Guid, Order>(new Order { OrderNumber = "ORD-1001" })!;

// String key — must be supplied by you
db.Insert<string, ApiKey>(new ApiKey { Id = "ak_live_abc", OwnerId = userId });
```

After insert with an integer/identity key, the entity's key property is updated with the generated identity value. If a `[Version]` property exists and is `0`, it is initialised to `1`.

### BatchInsert

`Insert` costs one round trip per entity, so inserting a collection of any size spends its time in
latency rather than in the insert. `BatchInsertAsync` sends many rows per statement and returns the
number of rows written:

```csharp
var orders = Enumerable.Range(0, 5_000)
    .Select(i => new Order { OrderNumber = $"ORD-{i}" })
    .ToArray();

int written = await db.BatchInsertAsync(orders, cancellationToken: ct);
```

Columns are derived exactly as they are for a single insert, so `[Table]`, `[Column]`,
`[IgnoreInsert]` and the rest behave identically, and empty `Guid` keys are filled in per row.

Rows are sent in batches, because providers cap how many parameters one statement may carry — SQL
Server allows 2100. The default batch size divides a conservative budget by the column count; pass
`batchSize` where a provider is tighter:

```csharp
await db.BatchInsertAsync(orders, batchSize: 200, cancellationToken: ct);
```

Multi-row `VALUES` is used rather than a provider-specific bulk copy, so this stays dialect-aware
without QueryKit taking a dependency on any one provider. Oracle has no multi-row `VALUES` clause
and gets `INSERT ALL ... SELECT 1 FROM dual` instead; nothing in your code changes.

Pass an `IDbTransaction` to have the whole batch roll back together:

```csharp
using var tx = conn.BeginTransaction();
await conn.BatchInsertAsync(orders, tx, cancellationToken: ct);
tx.Commit();
```

> **Identity keys need an opt-in.** The rows insert fine — the identity column is left out of the
> statement exactly as it is for a single insert — but no dialect returns many generated keys from
> one statement, so the entities' key properties cannot be populated. Rather than hand back objects
> whose `Id` is silently still `0`, `BatchInsertAsync` throws unless you say you don't need them:
>
> ```csharp
> await db.BatchInsertAsync(auditRows, discardGeneratedKeys: true, cancellationToken: ct);
> ```
>
> Use it for rows nothing reads back by key. If you do need the keys, insert those rows with
> `InsertAsync`.


### Update

Updates every mapped column except keys, `[ReadOnly]`, and `[IgnoreUpdate]`:

```csharp
ada.LastName = "King-Lovelace";
int rows = db.Update(ada);
```

### Optimistic concurrency with `UpdateWithVersion`

Add a `long Version` column (or any `long` property tagged `[Version]`):

```csharp
public class Document
{
    [Key] public int Id { get; set; }
    public string Title { get; set; } = "";
    [Version] public long Revision { get; set; }
}
```

Then update with the version you originally read:

```csharp
var doc = db.Get<Document>(id)!;
doc.Title = "New title";

int rows = db.UpdateWithVersion(doc, expectedVersion: doc.Revision);
if (rows == 0)
    throw new DBConcurrencyException("Document was modified by someone else.");
```

QueryKit appends `Revision = Revision + 1` to the `SET` clause and `AND Revision = @ExpectedVersion` to the `WHERE` clause, so the update only succeeds if the row still has the version you expected.

### Delete

```csharp
// By entity (uses the key property values)
db.Delete(ada);

// By primary-key value
db.Delete<User>(42);

// Composite key
db.Delete<OrderLine>(new { OrderId = orderId, LineNo = 3 });
```

### DeleteList

```csharp
// Anonymous filter — at least one property is required to prevent accidental full-table deletes
db.DeleteList<User>(new { IsActive = false });

// Raw WHERE fragment with parameters
db.DeleteList<User>(
    "where CreatedUtc < @cutoff",
    new { cutoff = DateTime.UtcNow.AddYears(-2) });

// Truly delete every row — opt in explicitly
db.DeleteList<User>("1=1");
```

Calling `DeleteList<T>(null)` or with an empty filter object throws — full-table deletes have to be intentional.

---

## Stored procedures

```csharp
var orders = db.ExecuteStoredProcedure<Order>(
    "dbo.GetOrdersForUser",
    new { UserId = userId });
```

The async version (`ExecuteStoredProcedureAsync<T>`) follows the same shape.

---

## Async API

Every CRUD method has an async counterpart that accepts a `CancellationToken`:

```csharp
var user  = await db.GetAsync<User>(id, cancellationToken: ct);
var users = await db.GetListAsync<User>(new { IsActive = true }, cancellationToken: ct);
var page  = await db.GetListPagedAsync<User>(1, 25, "", "Id ASC", cancellationToken: ct);
var newId = await db.InsertAsync<int, User>(user, cancellationToken: ct);
var rows  = await db.BatchInsertAsync(manyUsers, cancellationToken: ct);
await db.UpdateAsync(user, cancellationToken: ct);
await db.UpdateWithVersionAsync(doc, expectedVersion: doc.Revision, cancellationToken: ct);
await db.DeleteAsync<User>(id, cancellationToken: ct);
var n = await db.RecordCountAsync<User>(cancellationToken: ct);
```

The async overloads emit identical SQL to their sync counterparts.

---

## Choosing a dialect

Set the dialect once at startup:

```csharp
ConnectionExtensions.UseDialect(Dialect.PostgreSQL);
```

| Dialect      | Identifier quoting | Identity retrieval                | Paging                       |
| ------------ | ------------------ | --------------------------------- | ---------------------------- |
| `SQLServer`  | `[col]`            | `SCOPE_IDENTITY()`                | `OFFSET … FETCH NEXT`        |
| `PostgreSQL` | `"col"`            | `LASTVAL()`                       | `LIMIT … OFFSET …`           |
| `SQLite`     | `"col"`            | `LAST_INSERT_ROWID()`             | `LIMIT … OFFSET …`           |
| `MySQL`      | `` `col` ``        | `LAST_INSERT_ID()`                | `LIMIT … OFFSET …`           |
| `Oracle`     | `"col"`            | (none — supply your own keys)     | `ROWNUM` window              |
| `DB2`        | `"col"`            | `IDENTITY_VAL_LOCAL()`            | `ROW_NUMBER() OVER(...)`     |

`UseDialect` clears QueryKit's column allowlist cache, so it's safe to call again — but the expectation is to call it once during process startup.

---

## Composite keys

Mark every key property with `[Key]`. Pass keys as anonymous objects to `Get`, `Delete`, etc.:

```csharp
public class OrderLine
{
    [Key] public Guid OrderId { get; set; }
    [Key] public int  LineNo  { get; set; }
    public int  ProductId { get; set; }
    public int  Quantity  { get; set; }
}

var line = db.Get<OrderLine>(new { OrderId = orderId, LineNo = 3 });
db.Delete<OrderLine>(new { OrderId = orderId, LineNo = 3 });
```

---

## Transactions

Every CRUD method takes an optional `IDbTransaction`:

```csharp
using var tx = db.BeginTransaction();

var orderId = db.Insert<int, Order>(new Order { OrderNumber = "ORD-1" }, transaction: tx);
db.Insert(new OrderLine { OrderId = orderId, LineNo = 1, ProductId = 7, Quantity = 2 }, transaction: tx);
db.Insert(new OrderLine { OrderId = orderId, LineNo = 2, ProductId = 9, Quantity = 1 }, transaction: tx);

tx.Commit();
```

---

## Logging generated SQL

By default QueryKit writes generated SQL to `System.Diagnostics.Trace` while a debugger is attached. You can hook in your own logger:

```csharp
// Microsoft.Extensions.Logging
ConnectionExtensions.Logger = msg => logger.LogDebug(msg);

// Or silence it entirely
ConnectionExtensions.Logger = null;
```

The logger is invoked for every CRUD method with the SQL that QueryKit built (parameters are bound by Dapper, not interpolated into the string).

---

## Safety notes

- **`ORDER BY` is allowlisted.** Raw `orderBy` strings to `GetList` and `GetListPaged` are validated against the entity's mapped columns and property names — invalid tokens throw `ArgumentException`. Direction is restricted to `ASC` / `DESC`.
- **`DeleteList` refuses empty filters.** Calling it without a filter throws. To delete every row, pass `"1=1"` explicitly so the intent is in your code.
- **Identifiers are quoted per-dialect**, with the dialect's escape character doubled inside identifiers (e.g. `]` → `]]` in SQL Server).
- **`Insert` will populate empty Guid keys** with a sequential GUID, but throws on a missing string key — string keys must be supplied by you.

---

## License

See [LICENSE](LICENSE).
