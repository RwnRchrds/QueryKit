using System.ComponentModel.DataAnnotations.Schema;

namespace QueryKit.Tests.Data;

[Table("Persons")]
public class Person
{
    public Guid Id { get; set; }
    public string FirstName { get; set; } = null!;
    public string LastName  { get; set; } = null!;
    public int Age { get; set; }
}