namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Models
{
    public class AzureOpenAIOptions
    {
        public string ApiKey { get; set; } = string.Empty;
        public string Endpoint { get; set; } = string.Empty;
        public string ApiVersion { get; set; } = "2024-08-01-preview";
        public List<ModelTokenCost> ModelTokenCosts { get; set; } = new List<ModelTokenCost>();
    }

    public class ModelTokenCost
    {
        public string Model { get; set; } = string.Empty;
        public int CreditsPerToken { get; set; }
    }
}
