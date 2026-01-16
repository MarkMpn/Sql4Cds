using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Models
{
    public class TokenUsage
    {
        [Key]
        public int Id { get; set; }

        public int? UserId { get; set; }

        public int? OrganizationId { get; set; }

        [Required]
        public DateOnly UsageDate { get; set; }

        public int TokensUsed { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public DateTime LastUpdatedAt { get; set; } = DateTime.UtcNow;

        // Navigation properties
        [ForeignKey(nameof(UserId))]
        public User? User { get; set; }

        [ForeignKey(nameof(OrganizationId))]
        public Organization? Organization { get; set; }
    }
}
