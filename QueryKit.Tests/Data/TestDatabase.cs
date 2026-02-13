using System;
using System.Data;
using Dapper;
using Microsoft.Data.Sqlite;

namespace QueryKit.Tests.Data;

public static class TestDatabase
{
    public static IDbConnection OpenAndInit()
    {
        try
        {
            var conn = new SqliteConnection("Data Source=:memory:");
            conn.Open();

            // Base table used by existing tests
            conn.Execute(@"
CREATE TABLE Persons (
    Id        TEXT PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName  TEXT NOT NULL,
    Age       INTEGER NOT NULL
);");

            // Versioned (convention: property named 'Version')
            conn.Execute(@"
CREATE TABLE PersonWithVersion (
    Id        TEXT PRIMARY KEY,
    Version   INTEGER NOT NULL,
    FirstName TEXT NOT NULL,
    LastName  TEXT NOT NULL,
    Age       INTEGER NOT NULL
);");

            // Versioned (attribute: property named 'Revision' in CLR; column is 'Revision')
            conn.Execute(@"
CREATE TABLE PersonWithAttributedVersion (
    Id        TEXT PRIMARY KEY,
    Revision  INTEGER NOT NULL,
    FirstName TEXT NOT NULL
);");

            // Nullable version table (for long-only enforcement test)
            // NOTE: SQLite itself allows NULLs, but QueryKit should reject long? at runtime.
            conn.Execute(@"
CREATE TABLE PersonWithNullableVersion (
    Id        TEXT PRIMARY KEY,
    Revision  INTEGER NULL
);");

            // Multiple version props table (for multiple [Version] attributes test)
            conn.Execute(@"
CREATE TABLE PersonWithTwoVersions (
    Id    TEXT PRIMARY KEY,
    RevA  INTEGER NOT NULL,
    RevB  INTEGER NOT NULL
);");

            return conn;
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
            throw;
        }
    }

    internal static void Seed(IDbConnection conn)
    {
        var people = new[]
        {
            new Person { Id = Guid.NewGuid(), FirstName = "Ada",    LastName = "Lovelace", Age = 36 },
            new Person { Id = Guid.NewGuid(), FirstName = "Alan",   LastName = "Turing",   Age = 41 },
            new Person { Id = Guid.NewGuid(), FirstName = "Grace",  LastName = "Hopper",   Age = 85 },
            new Person { Id = Guid.NewGuid(), FirstName = "Edsger", LastName = "Dijkstra", Age = 72 },
        };

        conn.Execute(
            "INSERT INTO Persons (Id, FirstName, LastName, Age) VALUES (@Id,@FirstName,@LastName,@Age)",
            people);
    }
}