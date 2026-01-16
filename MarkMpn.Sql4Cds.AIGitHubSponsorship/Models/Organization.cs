using System.ComponentModel.DataAnnotations;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Models
{
    public class Organization
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [MaxLength(100)]
        public string GitHubLogin { get; set; } = string.Empty;

        [MaxLength(500)]
        public string AvatarUrl { get; set; } = string.Empty;

        public int TokensAllowedPerMonth { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public DateTime LastUpdatedAt { get; set; } = DateTime.UtcNow;

        // Navigation properties
        public ICollection<OrganizationMember> Members { get; set; } = new List<OrganizationMember>();
        public ICollection<TokenUsage> TokenUsages { get; set; } = new List<TokenUsage>();
    }
}
