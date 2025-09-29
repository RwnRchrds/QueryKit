using System.Data;
using Dapper;

namespace QueryKit.Tests.Data;

public sealed class SqliteGuidHandler : SqlMapper.TypeHandler<Guid>
{
    public override void SetValue(IDbDataParameter parameter, Guid value)
        => parameter.Value = value.ToString(); // TEXT column

    public override Guid Parse(object value)
        => value is Guid g ? g : Guid.Parse((string)value);
}

public sealed class SqliteNullableGuidHandler : SqlMapper.TypeHandler<Guid?>
{
    public override void SetValue(IDbDataParameter parameter, Guid? value)
        => parameter.Value = value?.ToString() ?? (object)DBNull.Value;

    public override Guid? Parse(object value)
        => value is null || value is DBNull ? (Guid?)null
            : value is Guid g ? g
            : Guid.Parse((string)value);
}