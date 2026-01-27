using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Services
{
    public class SponsorshipInfo
    {
        public string Login { get; set; } = string.Empty;
        public bool IsOrganization { get; set; }
        public int TokensAllowed { get; set; }
        public string AvatarUrl { get; set; } = string.Empty;
    }

    public class OrganizationInfo
    {
        public string Login { get; set; } = string.Empty;
        public string AvatarUrl { get; set; } = string.Empty;
    }

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

        public async Task<List<SponsorshipInfo>> GetAllSponsorships(string accessToken)
        {
            const string sponsorableLogin = "MarkMpn"; // Owner of SQL 4 CDS project
            var sponsorships = new List<SponsorshipInfo>();

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
                                ... on User { 
                                    login
                                    avatarUrl
                                }
                                ... on Organization { 
                                    login
                                    avatarUrl
                                }
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
                return sponsorships;
            }

            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

            if (doc.RootElement.TryGetProperty("errors", out var errorsElement))
            {
                _logger.LogWarning("GitHub Sponsors API returned errors: {Errors}", errorsElement.ToString());
                return sponsorships;
            }

            if (!doc.RootElement.TryGetProperty("data", out var data) ||
                !data.TryGetProperty("viewer", out var viewer) ||
                !viewer.TryGetProperty("sponsorshipsAsSponsor", out var sponsorshipsList) ||
                !sponsorshipsList.TryGetProperty("nodes", out var nodes))
            {
                _logger.LogWarning("Unexpected Sponsors API response shape");
                return sponsorships;
            }

            foreach (var node in nodes.EnumerateArray())
            {
                if (!node.TryGetProperty("sponsorable", out var sponsorableElement))
                {
                    continue;
                }

                string? sponsorableLoginValue = null;
                string? avatarUrl = null;
                bool isOrganization = false;

                if (sponsorableElement.ValueKind == JsonValueKind.Object)
                {
                    if (sponsorableElement.TryGetProperty("login", out var loginProp))
                    {
                        sponsorableLoginValue = loginProp.GetString();
                    }
                    if (sponsorableElement.TryGetProperty("avatarUrl", out var avatarProp))
                    {
                        avatarUrl = avatarProp.GetString();
                    }
                    // Check if it's an organization by checking __typename if available
                    if (sponsorableElement.TryGetProperty("__typename", out var typeProp))
                    {
                        isOrganization = typeProp.GetString() == "Organization";
                    }
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
                var tokens = MapTierToTokens(monthlyCents, isOrganization);
                
                sponsorships.Add(new SponsorshipInfo
                {
                    Login = sponsorableLoginValue,
                    IsOrganization = isOrganization,
                    TokensAllowed = tokens,
                    AvatarUrl = avatarUrl ?? ""
                });

                _logger.LogInformation($"Sponsor detected: {sponsorableLoginValue} ({(isOrganization ? "Org" : "User")}) at {monthlyCents} cents => {tokens} credits/month");
            }

            return sponsorships;
        }

        public async Task<int> DetermineTokenAllowance(string accessToken)
        {
            var sponsorships = await GetAllSponsorships(accessToken);
            // For individual users, return their personal sponsorship tokens (orgs handled separately)
            var userSponsorship = sponsorships.FirstOrDefault(s => !s.IsOrganization);
            return userSponsorship?.TokensAllowed ?? 0;
        }

        public async Task<List<OrganizationInfo>> GetOrganizationMemberships(string accessToken)
        {
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", accessToken);
            client.DefaultRequestHeaders.UserAgent.ParseAdd("SQL4CDS-AI-Sponsorship");
            client.DefaultRequestHeaders.Accept.ParseAdd("application/vnd.github+json");
            client.DefaultRequestHeaders.Add("X-GitHub-Api-Version", "2022-11-28");

            var query = @"query {
                viewer {
                    organizations(first: 100) {
                        nodes {
                            login
                            avatarUrl
                        }
                    }
                }
            }";

            var requestBody = new { query };
            var content = new StringContent(JsonSerializer.Serialize(requestBody), Encoding.UTF8, "application/json");

            var response = await client.PostAsync("https://api.github.com/graphql", content);

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogWarning($"GitHub organizations query failed with status {response.StatusCode}");
                return new List<OrganizationInfo>();
            }

            using var doc = JsonDocument.Parse(await response.Content.ReadAsStringAsync());

            if (doc.RootElement.TryGetProperty("errors", out var errorsElement))
            {
                _logger.LogWarning("GitHub organizations API returned errors: {Errors}", errorsElement.ToString());
                return new List<OrganizationInfo>();
            }

            if (!doc.RootElement.TryGetProperty("data", out var data) ||
                !data.TryGetProperty("viewer", out var viewer) ||
                !viewer.TryGetProperty("organizations", out var orgs) ||
                !orgs.TryGetProperty("nodes", out var nodes))
            {
                _logger.LogWarning("Unexpected organizations API response shape");
                return new List<OrganizationInfo>();
            }

            var organizations = new List<OrganizationInfo>();
            foreach (var node in nodes.EnumerateArray())
            {
                if (node.TryGetProperty("login", out var loginProp))
                {
                    var login = loginProp.GetString();
                    var avatarUrl = node.TryGetProperty("avatarUrl", out var avatarProp) ? avatarProp.GetString() : "";
                    
                    if (!string.IsNullOrEmpty(login))
                    {
                        organizations.Add(new OrganizationInfo
                        {
                            Login = login,
                            AvatarUrl = avatarUrl ?? ""
                        });
                    }
                }
            }

            return organizations;
        }

        public static int MapTierToTokens(int monthlyPriceInCents, bool isOrganization)
        {
            if (isOrganization)
            {
                // Organization tiers - minimum $100/month
                if (monthlyPriceInCents >= 10000) return 250_000_000; // $100+ -> 250M shared
                return 0;
            }
            else
            {
                // Individual tiers
                // Bronze: $5 -> 5M; Silver: $15 -> 20M; Gold: $50 -> 100M
                if (monthlyPriceInCents >= 5000) return 100_000_000;
                if (monthlyPriceInCents >= 1500) return 20_000_000;
                if (monthlyPriceInCents >= 500) return 5_000_000;
                return 0;
            }
        }

        public static string DescribeSponsorship(int tokensAllowed)
        {
            return tokensAllowed switch
            {
                >= 100_000_000 => "Gold ($50/mo) - 100M credits",
                >= 20_000_000 => "Silver ($15/mo) - 20M credits",
                >= 5_000_000 => "Bronze ($5/mo) - 5M credits",
                _ => "Not a sponsor yet"
            };
        }
    }
}
