using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Data;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Models;
using MarkMpn.Sql4Cds.AIGitHubSponsorship.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;

namespace MarkMpn.Sql4Cds.AIGitHubSponsorship.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class AIController : ControllerBase
    {
        private readonly ILogger<AIController> _logger;
        private readonly IConfiguration _configuration;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly ApplicationDbContext _context;
        private readonly GitHubSponsorshipService _sponsorshipService;

        public AIController(
            ILogger<AIController> logger,
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

        [HttpPost("chat/completions")]
        public async Task<IActionResult> ChatCompletions()
        {
            try
            {
                // Extract API key from Authorization header
                var authHeader = Request.Headers.Authorization.ToString();
                if (string.IsNullOrEmpty(authHeader) || !authHeader.StartsWith("Bearer "))
                {
                    return Unauthorized(new { error = new { message = "Missing or invalid Authorization header" } });
                }

                var userApiKey = authHeader.Substring("Bearer ".Length).Trim();

                // Validate API key and get user
                var user = await _context.Users
                    .FirstOrDefaultAsync(u => u.ApiKey == userApiKey);

                if (user == null)
                {
                    _logger.LogWarning($"Invalid API key attempted: {userApiKey.Substring(0, Math.Min(10, userApiKey.Length))}...");
                    return Unauthorized(new { error = new { message = "Invalid API key" } });
                }

                // Check if this is the first request of the month and refresh sponsorship status if needed
                var currentMonth = DateOnly.FromDateTime(DateTime.UtcNow);
                var firstDayOfMonth = new DateOnly(currentMonth.Year, currentMonth.Month, 1);

                var hasUsageThisMonth = await _context.TokenUsages
                    .AnyAsync(t => t.UserId == user.Id && t.UsageDate >= firstDayOfMonth);

                if (!hasUsageThisMonth)
                {
                    // First request of the month - refresh sponsorship status
                    try
                    {
                        var newTokensAllowed = await _sponsorshipService.DetermineTokenAllowance(user.AccessToken);
                        if (newTokensAllowed != user.TokensAllowedPerMonth)
                        {
                            user.TokensAllowedPerMonth = newTokensAllowed;
                            user.LastUpdatedAt = DateTime.UtcNow;
                            await _context.SaveChangesAsync();
                            _logger.LogInformation($"Updated sponsorship for user {user.GitHubUsername}: {newTokensAllowed} tokens/month");
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogWarning(ex, $"Failed to refresh sponsorship status for user {user.GitHubUsername}");
                        // Continue with existing allowance if refresh fails
                    }
                }

                // Check remaining credits
                var tokensUsedThisMonth = await _context.TokenUsages
                    .Where(t => t.UserId == user.Id && t.UsageDate >= firstDayOfMonth)
                    .SumAsync(t => t.TokensUsed);

                var tokensRemaining = user.TokensAllowedPerMonth - tokensUsedThisMonth;

                if (tokensRemaining <= 0)
                {
                    _logger.LogWarning($"User {user.GitHubUsername} has no remaining credits");
                    return StatusCode(429, new { error = new { message = "Insufficient credits. Please upgrade your sponsorship tier.", type = "insufficient_quota" } });
                }

                // Read the request body
                string requestBody;
                using (var reader = new StreamReader(Request.Body))
                {
                    requestBody = await reader.ReadToEndAsync();
                }

                // Get OpenAI configuration
                var openAiApiKey = _configuration["OpenAI:ApiKey"];
                var openAiBaseUrl = _configuration["OpenAI:BaseUrl"] ?? "https://api.openai.com/v1";

                if (string.IsNullOrEmpty(openAiApiKey))
                {
                    _logger.LogError("OpenAI API key not configured");
                    return StatusCode(500, new { error = new { message = "AI service not configured" } });
                }

                // Forward request to OpenAI
                var client = _httpClientFactory.CreateClient();
                var request = new HttpRequestMessage(HttpMethod.Post, $"{openAiBaseUrl}/chat/completions");
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", openAiApiKey);
                request.Headers.Add("User-Agent", "SQL4CDS-AI-Sponsorship");
                request.Content = new StringContent(requestBody, Encoding.UTF8, "application/json");

                var response = await client.SendAsync(request, HttpCompletionOption.ResponseHeadersRead);

                if (!response.IsSuccessStatusCode)
                {
                    var errorContent = await response.Content.ReadAsStringAsync();
                    _logger.LogError($"OpenAI API error: {response.StatusCode} - {errorContent}");
                    return StatusCode((int)response.StatusCode, errorContent);
                }

                // Check if it's a streaming response
                var requestJson = JsonSerializer.Deserialize<JsonElement>(requestBody);
                var isStreaming = requestJson.TryGetProperty("stream", out var streamProp) && streamProp.GetBoolean();

                if (isStreaming)
                {
                    return await HandleStreamingResponse(response, user);
                }
                else
                {
                    return await HandleNonStreamingResponse(response, user);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error processing AI request");
                return StatusCode(500, new { error = new { message = "Internal server error" } });
            }
        }

        private async Task<IActionResult> HandleNonStreamingResponse(HttpResponseMessage response, User user)
        {
            var responseContent = await response.Content.ReadAsStringAsync();
            var responseJson = JsonSerializer.Deserialize<JsonElement>(responseContent);

            // Extract token usage
            int tokensUsed = 0;
            if (responseJson.TryGetProperty("usage", out var usage))
            {
                if (usage.TryGetProperty("total_tokens", out var totalTokens))
                {
                    tokensUsed = totalTokens.GetInt32();
                }
            }

            // Record token usage
            if (tokensUsed > 0)
            {
                await RecordTokenUsage(user.Id, tokensUsed);
                _logger.LogInformation($"User {user.GitHubUsername} used {tokensUsed} tokens");
            }

            // Return the response
            Response.ContentType = "application/json";
            return Content(responseContent, "application/json");
        }

        private async Task<IActionResult> HandleStreamingResponse(HttpResponseMessage response, User user)
        {
            Response.ContentType = "text/event-stream";
            Response.Headers.CacheControl = "no-cache";
            Response.Headers.Add("X-Accel-Buffering", "no");

            var responseStream = await response.Content.ReadAsStreamAsync();
            var reader = new StreamReader(responseStream);
            var writer = Response.Body;

            int tokensUsed = 0;
            string line;

            while ((line = await reader.ReadLineAsync()) != null)
            {
                if (string.IsNullOrWhiteSpace(line))
                {
                    await writer.WriteAsync(Encoding.UTF8.GetBytes("\n"));
                    await writer.FlushAsync();
                    continue;
                }

                // Parse SSE data
                if (line.StartsWith("data: "))
                {
                    var data = line.Substring(6);

                    if (data == "[DONE]")
                    {
                        await writer.WriteAsync(Encoding.UTF8.GetBytes(line + "\n\n"));
                        await writer.FlushAsync();
                        break;
                    }

                    try
                    {
                        var json = JsonSerializer.Deserialize<JsonElement>(data);

                        // Extract usage information if available (usually in the last chunk)
                        if (json.TryGetProperty("usage", out var usage))
                        {
                            if (usage.TryGetProperty("total_tokens", out var totalTokens))
                            {
                                tokensUsed = totalTokens.GetInt32();
                            }
                        }
                    }
                    catch
                    {
                        // Ignore parsing errors for individual chunks
                    }
                }

                // Stream the line to the client
                await writer.WriteAsync(Encoding.UTF8.GetBytes(line + "\n"));
                await writer.FlushAsync();
            }

            // Record token usage if we captured it
            if (tokensUsed > 0)
            {
                await RecordTokenUsage(user.Id, tokensUsed);
                _logger.LogInformation($"User {user.GitHubUsername} used {tokensUsed} tokens (streaming)");
            }
            else
            {
                // For streaming responses, OpenAI may not include usage in the stream
                // We'll need to estimate or make a separate API call to get usage
                _logger.LogWarning($"Could not determine token usage for streaming request from user {user.GitHubUsername}");
            }

            return new EmptyResult();
        }

        private async Task RecordTokenUsage(int userId, int tokensUsed)
        {
            var today = DateOnly.FromDateTime(DateTime.UtcNow);
            var now = DateTime.UtcNow;

            // Try to update existing record first (atomic operation)
            var rowsAffected = await _context.TokenUsages
                .Where(t => t.UserId == userId && t.UsageDate == today)
                .ExecuteUpdateAsync(setters => setters
                    .SetProperty(t => t.TokensUsed, t => t.TokensUsed + tokensUsed)
                    .SetProperty(t => t.LastUpdatedAt, now));

            if (rowsAffected > 0)
            {
                // Successfully updated existing record
                return;
            }

            // No existing record - try to insert
            try
            {
                var usage = new TokenUsage
                {
                    UserId = userId,
                    TokensUsed = tokensUsed,
                    UsageDate = today,
                    CreatedAt = now,
                    LastUpdatedAt = now
                };
                _context.TokenUsages.Add(usage);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateException)
            {
                // Race condition: another thread inserted between our update and insert
                // Try update again
                await _context.TokenUsages
                    .Where(t => t.UserId == userId && t.UsageDate == today)
                    .ExecuteUpdateAsync(setters => setters
                        .SetProperty(t => t.TokensUsed, t => t.TokensUsed + tokensUsed)
                        .SetProperty(t => t.LastUpdatedAt, DateTime.UtcNow));
            }
        }
    }
}
