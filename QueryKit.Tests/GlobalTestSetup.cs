using Dapper;
using QueryKit.Tests.Data;

namespace QueryKit.Tests;

[SetUpFixture]
public class GlobalTestSetup
{
    [OneTimeSetUp]
    public void RegisterTypeHandlers()
    {
        SqlMapper.AddTypeHandler(new SqliteGuidHandler());
        SqlMapper.AddTypeHandler(new SqliteNullableGuidHandler());
    }
}