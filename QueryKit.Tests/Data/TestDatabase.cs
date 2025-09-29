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

            conn.Execute(@"
CREATE TABLE Persons (
    Id        TEXT PRIMARY KEY,
    FirstName TEXT NOT NULL,
    LastName  TEXT NOT NULL,
    Age       INTEGER NOT NULL
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
            "INSERT INTO Persons (Id, FirstName, LastName, Age) VALUES (@Id,@FirstName,@LastName,@Age)", people);
    }
}