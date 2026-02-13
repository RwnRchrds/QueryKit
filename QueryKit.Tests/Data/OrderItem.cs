using System.ComponentModel.DataAnnotations.Schema;
using QueryKit.Attributes;

namespace QueryKit.Tests.Data;

[Table("OrderItems")]
public class OrderItem
{
    [Key] public Guid OrderId { get; set; }
    [Key] public int  LineNumber { get; set; }
    public string? Sku { get; set; }
}