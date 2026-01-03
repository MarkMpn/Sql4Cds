using System.Diagnostics;
using System.Linq;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Data;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Models;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ApplicationDbContext _context;
        private readonly GitHubSponsorshipService _sponsorshipService;

        public HomeController(
            ILogger<HomeController> logger, 
            IConfiguration configuration, 
            IHttpClientFactory httpClientFactory,
            ApplicationDbContext context,
            GitHubSponsorshipService sponsorshipService)
        {
            _logger = logger;
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
            _context = context;
            _sponsorshipService = sponsorshipService;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult GitHubLogin()
        {
            var clientId = _configuration["GitHub:ClientId"];
            var redirectUri = _configuration["GitHub:RedirectUri"];
            var scopes = _configuration["GitHub:Scopes"];
            var state = Guid.NewGuid().ToString();

            // Store state in session to verify callback
            HttpContext.Session.SetString("GitHubOAuthState", state);

            var authUrl = $"https://github.com/login/oauth/authorize?client_id={clientId}&redirect_uri={Uri.EscapeDataString(redirectUri)}&scope={Uri.EscapeDataString(scopes)}&state={state}";

            return Redirect(authUrl);
        }

        public async Task<IActionResult> GitHubCallback(string code, string state)
        {
            // Verify state to prevent CSRF
            var storedState = HttpContext.Session.GetString("GitHubOAuthState");
            if (string.IsNullOrEmpty(storedState) || storedState != state)
            {
                _logger.LogWarning("OAuth state mismatch. Possible CSRF attack.");
                return RedirectToAction("Error");
            }

            if (string.IsNullOrEmpty(code))
            {
                _logger.LogError("No code received from GitHub OAuth");
                return RedirectToAction("Error");
            }

            try
            {
                // Exchange code for access token
                var accessToken = await ExchangeCodeForAccessToken(code);
                
                if (string.IsNullOrEmpty(accessToken))
                {
                    _logger.LogError("Failed to obtain access token from GitHub");
                    return RedirectToAction("Error");
                }

                // Store access token in session
                HttpContext.Session.SetString("GitHubAccessToken", accessToken);

                // Get user information
                var userInfo = await GetGitHubUserInfo(accessToken);
                if (userInfo != null)
                {
                    var username = userInfo.Value.GetProperty("login").GetString() ?? "";
                    var avatarUrl = userInfo.Value.GetProperty("avatar_url").GetString() ?? "";
                    HttpContext.Session.SetString("GitHubUsername", username);
                    _logger.LogInformation($"User {username} authenticated successfully");

                    // Determine sponsorship level and allowed credits
                    var tokensAllowed = await _sponsorshipService.DetermineTokenAllowance(accessToken);

                    // Create or update user in database
                    await CreateOrUpdateUser(username, accessToken, tokensAllowed, avatarUrl);
                }

                // TODO: Check sponsorship status
                // var sponsorshipLevel = await CheckSponsorshipLevel(accessToken);

                return RedirectToAction("Dashboard");
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error during GitHub OAuth callback");
                return RedirectToAction("Error");
            }
        }

        private async Task CreateOrUpdateUser(string username, string accessToken, int tokensAllowed, string avatarUrl = "")
        {
            var user = await _context.Users
                .FirstOrDefaultAsync(u => u.GitHubUsername == username);

            if (user == null)
            {
                // Create new user with default token allowance
                user = new User
                {
                    GitHubUsername = username,
                    AccessToken = accessToken,
                    ApiKey = GenerateApiKey(),
                    AvatarUrl = avatarUrl,
                    TokensAllowedPerMonth = tokensAllowed,
                    CreatedAt = DateTime.UtcNow,
                    LastUpdatedAt = DateTime.UtcNow
                };
                _context.Users.Add(user);
                _logger.LogInformation($"Created new user: {username}");
            }
            else
            {
                // Update existing user
                user.AccessToken = accessToken;
                user.TokensAllowedPerMonth = tokensAllowed;
                if (!string.IsNullOrWhiteSpace(avatarUrl))
                {
                    user.AvatarUrl = avatarUrl;
                }
                if (string.IsNullOrWhiteSpace(user.ApiKey))
                {
                    user.ApiKey = GenerateApiKey();
                }
                user.LastUpdatedAt = DateTime.UtcNow;
                _logger.LogInformation($"Updated existing user: {username}");
            }

            await _context.SaveChangesAsync();
        }

        private async Task<string?> ExchangeCodeForAccessToken(string code)
        {
            var clientId = _configuration["GitHub:ClientId"];
            var clientSecret = _configuration["GitHub:ClientSecret"];
            var redirectUri = _configuration["GitHub:RedirectUri"];

            var client = _httpClientFactory.CreateClient();
            var requestBody = new
            {
                client_id = clientId,
                client_secret = clientSecret,
                code = code,
                redirect_uri = redirectUri
            };

            var content = new StringContent(
                JsonSerializer.Serialize(requestBody),
                Encoding.UTF8,
                "application/json"
            );

            client.DefaultRequestHeaders.Add("Accept", "application/json");
            client.DefaultRequestHeaders.Add("User-Agent", "SQL4CDS-AI-Sponsorship");

            var response = await client.PostAsync("https://github.com/login/oauth/access_token", content);
            
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError($"Failed to exchange code for token. Status: {response.StatusCode}");
                return null;
            }

            var responseContent = await response.Content.ReadAsStringAsync();
            var tokenResponse = JsonSerializer.Deserialize<JsonElement>(responseContent);
            
            return tokenResponse.TryGetProperty("access_token", out var token) ? token.GetString() : null;
        }

        private async Task<JsonElement?> GetGitHubUserInfo(string accessToken)
        {
            var client = _httpClientFactory.CreateClient();
            client.DefaultRequestHeaders.Add("Authorization", $"Bearer {accessToken}");
            client.DefaultRequestHeaders.Add("User-Agent", "SQL4CDS-AI-Sponsorship");

            var response = await client.GetAsync("https://api.github.com/user");
            
            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError($"Failed to get user info. Status: {response.StatusCode}");
                return null;
            }

            var content = await response.Content.ReadAsStringAsync();
            return JsonSerializer.Deserialize<JsonElement>(content);
        }

        private static string GenerateApiKey()
        {
            // 48 bytes -> 64 base64 chars; prefix with sk_
            var bytes = RandomNumberGenerator.GetBytes(48);
            var token = Convert.ToBase64String(bytes)
                .Replace('+', '-')
                .Replace('/', '_')
                .TrimEnd('=');
            return $"sk_{token}";
        }

        public async Task<IActionResult> Dashboard()
        {
            var accessToken = HttpContext.Session.GetString("GitHubAccessToken");
            var username = HttpContext.Session.GetString("GitHubUsername");

            if (string.IsNullOrEmpty(accessToken) || string.IsNullOrEmpty(username))
            {
                return RedirectToAction(nameof(GitHubLogin));
            }

            // Get user from database
            var user = await _context.Users
                .Include(u => u.TokenUsages)
                .FirstOrDefaultAsync(u => u.GitHubUsername == username);

            if (user != null)
            {
                ViewBag.Username = username;
                ViewBag.AvatarUrl = user.AvatarUrl;
                ViewBag.TokensAllowed = user.TokensAllowedPerMonth;
                ViewBag.SponsorshipLevel = GitHubSponsorshipService.DescribeSponsorship(user.TokensAllowedPerMonth);
                ViewBag.ApiKey = user.ApiKey;
                
                // Calculate tokens used this month
                var currentMonth = DateOnly.FromDateTime(DateTime.UtcNow);
                var firstDayOfMonth = new DateOnly(currentMonth.Year, currentMonth.Month, 1);
                
                var tokensUsedThisMonth = await _context.TokenUsages
                    .Where(t => t.UserId == user.Id && t.UsageDate >= firstDayOfMonth)
                    .SumAsync(t => t.TokensUsed);
                
                ViewBag.TokensUsedThisMonth = tokensUsedThisMonth;
                ViewBag.TokensRemaining = Math.Max(0, user.TokensAllowedPerMonth - tokensUsedThisMonth);
            }
            else
            {
                ViewBag.Username = username;
                ViewBag.AvatarUrl = "";
                ViewBag.TokensAllowed = 0;
                ViewBag.TokensUsedThisMonth = 0;
                ViewBag.TokensRemaining = 0;
                ViewBag.SponsorshipLevel = GitHubSponsorshipService.DescribeSponsorship(0);
                ViewBag.ApiKey = "sk_pending_verification";
            }

            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> RefreshSponsorship()
        {
            var accessToken = HttpContext.Session.GetString("GitHubAccessToken");
            var username = HttpContext.Session.GetString("GitHubUsername");

            if (string.IsNullOrEmpty(accessToken) || string.IsNullOrEmpty(username))
            {
                return RedirectToAction("Index");
            }

            var tokensAllowed = await _sponsorshipService.DetermineTokenAllowance(accessToken);
            await CreateOrUpdateUser(username, accessToken, tokensAllowed);

            return RedirectToAction("Dashboard");
        }

        public IActionResult Logout()
        {
            HttpContext.Session.Clear();
            return RedirectToAction("Index");
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
