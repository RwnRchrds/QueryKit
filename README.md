# QueryKit

> Lightweight, dependency-light SQL builder + CRUD extensions for Dapper.  
> Dialect-aware. Attribute-friendly. Zero ceremony.

- **NuGet**: `QueryKit`
- **Targets**: `net8.0` (recommended) + optional `netstandard2.0`
- **Databases**: SQL Server, PostgreSQL, MySQL/MariaDB, SQLite, Oracle, Db2

---

## Why QueryKit?

- **Tiny surface** – just `IDbConnection` extension methods (sync + async).
- **Predictable SQL** – no LINQ provider, no magic; you can log every statement.
- **Dialect-aware** – identifier quoting, identity retrieval, and paging per DB.
- **Attribute-friendly** – `[Key]`, `[Column]`, `[NotMapped]`, `[IgnoreInsert]`, `[IgnoreUpdate]`, etc.
- **Batteries included** – paging, composite keys, anonymous filters, raw `WHERE` support.

---

## Install

```bash
dotnet add package QueryKit
