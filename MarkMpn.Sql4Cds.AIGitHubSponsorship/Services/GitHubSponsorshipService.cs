using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Services
{
    public class GitHubSponsorshipService
    {
        private readonly ILogger<GitHubSponsorshipService> _logger;
        private readonly IHttpClientFactory _httpClientFactory;

        public GitHubSponsorshipService(
            ILogger<GitHubSponsorshipService> logger,
            IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _httpClientFactory = httpClientFactory;
        }

        public async Task<int> DetermineTokenAllowance(string accessToken)
        {
            const string sponsorableLogin = "MarkMpn"; // Owner of SQL 4 CDS project

            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            client.DefaultRequestHeaders.UserAgent.ParseAdd("SQL4CDS-AI-Sponsorship");
            client.DefaultRequestHeaders.Accept.ParseAdd("application/vnd.github+json");
            client.DefaultRequestHeaders.Add("X-GitHub-Api-Version", "2022-11-28");

            var query = @"query {
                viewer {
                    sponsorshipsAsSponsor(first: 50, activeOnly: true) {
                        nodes {
                            sponsorable {
                                ... on User { login }
                                ... on Organization { login }
                            }
                            tier { monthlyPriceInCents name }
                        }
                    }
                }
            }";

            var requestBody = new { query };
            var content = new StringContent(JsonSerializer.Serialize(requestBody), Encoding.UTF8, "application/json");

            var response = await client.PostAsync("https://api.github.com/graphql", content);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning($"GitHub Sponsors query failed with status {response.StatusCode}");
                return 0;
            }

            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

            if (doc.RootElement.TryGetProperty("errors", out var errorsElement))
            {
                _logger.LogWarning("GitHub Sponsors API returned errors: {Errors}", errorsElement.ToString());
                return 0;
            }

            if (!doc.RootElement.TryGetProperty("data", out var data) ||
                !data.TryGetProperty("viewer", out var viewer) ||
                !viewer.TryGetProperty("sponsorshipsAsSponsor", out var sponsorships) ||
                !sponsorships.TryGetProperty("nodes", out var nodes))
            {
                _logger.LogWarning("Unexpected Sponsors API response shape");
                return 0;
            }

            foreach (var node in nodes.EnumerateArray())
            {
                if (!node.TryGetProperty("sponsorable", out var sponsorableElement))
                {
                    continue;
                }

                string? sponsorableLoginValue = null;
                if (sponsorableElement.ValueKind == JsonValueKind.Object &&
                    sponsorableElement.TryGetProperty("login", out var loginProp))
                {
                    sponsorableLoginValue = loginProp.GetString();
                }

                if (string.IsNullOrEmpty(sponsorableLoginValue) ||
                    !string.Equals(sponsorableLoginValue, sponsorableLogin, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!node.TryGetProperty("tier", out var tier) || !tier.TryGetProperty("monthlyPriceInCents", out var price))
                {
                    continue;
                }

                var monthlyCents = price.GetInt32();
                var tokens = MapTierToTokens(monthlyCents);
                _logger.LogInformation($"Sponsor detected for {sponsorableLogin} at {monthlyCents} cents => {tokens} credits/month");
                return tokens;
            }

            return 0;
        }

        public static int MapTierToTokens(int monthlyPriceInCents)
        {
            // Map sponsorship tiers to AI credits per month
            // Bronze: $5 -> 5M; Silver: $15 -> 20M; Gold: $50 -> 100M
            if (monthlyPriceInCents >= 5000) return 100_000_000;
            if (monthlyPriceInCents >= 1500) return 20_000_000;
            if (monthlyPriceInCents >= 500) return 5_000_000;
            return 0;
        }

        public static string DescribeSponsorship(int tokensAllowed)
        {
            return tokensAllowed switch
            {
                >= 25000 => "Gold ($50/mo) - 100M credits",
                >= 5000 => "Silver ($15/mo) - 20M credits",
                >= 1000 => "Bronze ($5/mo) - 5M credits",
                _ => "Not a sponsor yet"
            };
        }
    }
}
