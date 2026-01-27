using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Models
{
    public class ManualCreditAssignment
    {
        [Key]
        public int Id { get; set; }

        public int? UserId { get; set; }

        public int? OrganizationId { get; set; }

        [Required]
        public int TokensAllowedPerMonth { get; set; }

        [MaxLength(500)]
        public string? Note { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public DateTime LastUpdatedAt { get; set; } = DateTime.UtcNow;

        // Navigation properties
        [ForeignKey(nameof(UserId))]
        public User? User { get; set; }

        [ForeignKey(nameof(OrganizationId))]
        public Organization? Organization { get; set; }
    }
}
