using System.ComponentModel.DataAnnotations.Schema;
using QueryKit.Attributes;

namespace QueryKit.Tests.Data;

[Table("Persons")]
public class Person
{
    public Guid Id { get; set; }
    public string FirstName { get; set; } = null!;
    public string LastName  { get; set; } = null!;
    public int Age { get; set; }
}

public class PersonWithVersion
{
    [Key]
    public Guid Id { get; set; }

    public long Version { get; set; }

    public string FirstName { get; set; } = default!;
    public string LastName  { get; set; } = default!;
    public int Age { get; set; }
}

public class PersonWithAttributedVersion
{
    [Key]
    public Guid Id { get; set; }

    [Version]
    public long Revision { get; set; }

    public string FirstName { get; set; } = default!;
}

public class PersonWithNullableVersion
{
    [Key]
    public Guid Id { get; set; }

    [Version]
    public long? Revision { get; set; }
}

public class PersonWithTwoVersions
{
    [Key]
    public Guid Id { get; set; }

    [Version]
    public long RevA { get; set; }

    [Version]
    public long RevB { get; set; }
}