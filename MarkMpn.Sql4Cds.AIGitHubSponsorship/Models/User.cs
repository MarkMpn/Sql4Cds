using System.ComponentModel.DataAnnotations;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Models
{
    public class User
    {
        [Key]
        public int Id { get; set; }

        [Required]
        [MaxLength(100)]
        public string GitHubUsername { get; set; } = string.Empty;

        [Required]
        [MaxLength(500)]
        public string AccessToken { get; set; } = string.Empty;

        public int TokensAllowedPerMonth { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        public DateTime LastUpdatedAt { get; set; } = DateTime.UtcNow;

        // Navigation property
        public ICollection<TokenUsage> TokenUsages { get; set; } = new List<TokenUsage>();
    }
}
