using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace QueryKit.Tests.Data;

[Table("OrderItems")]
public class OrderItem
{
    [Key] public Guid OrderId { get; set; }
    [Key] public int  LineNumber { get; set; }
    public string? Sku { get; set; }
}