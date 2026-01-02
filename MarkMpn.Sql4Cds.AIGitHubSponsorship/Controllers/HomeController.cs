using System.Diagnostics;
using System.Text;
using System.Text.Json;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Models;
using Microsoft.AspNetCore.Mvc;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;

        public HomeController(ILogger<HomeController> logger, IConfiguration configuration, IHttpClientFactory httpClientFactory)
        {
            _logger = logger;
            _configuration = configuration;
            _httpClientFactory = httpClientFactory;
        }

        public IActionResult Index()
        {
            return View();
        }

        public IActionResult Privacy()
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
                    HttpContext.Session.SetString("GitHubUsername", userInfo.Value.GetProperty("login").GetString() ?? "");
                    _logger.LogInformation($"User {userInfo.Value.GetProperty("login").GetString()} authenticated successfully");
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

        public IActionResult Dashboard()
        {
            var accessToken = HttpContext.Session.GetString("GitHubAccessToken");
            var username = HttpContext.Session.GetString("GitHubUsername");

            if (string.IsNullOrEmpty(accessToken))
            {
                return RedirectToAction("Index");
            }

            ViewBag.Username = username;
            return View();
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
