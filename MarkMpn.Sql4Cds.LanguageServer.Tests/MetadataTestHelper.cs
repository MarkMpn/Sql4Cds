using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    internal static class MetadataTestHelper
    {
        public static JObject Query(WebApiRequest request)
        {
            var query = request.Url.Substring(request.Url.IndexOf("@p1=", StringComparison.Ordinal) + 4).Split('&')[0];
            return JObject.Parse(Uri.UnescapeDataString(query));
        }

        public static WebApiResponse Response(params JObject[] entities)
        {
            return new WebApiResponse
            {
                Status = 200,
                Body = new JObject
                {
                    ["ServerVersionStamp"] = "stamp",
                    ["EntityMetadata"] = new JArray(entities)
                }.ToString()
            };
        }

        public static IAttributeMetadataCache Cache(params JObject[] entities)
        {
            return new PptbOrganizationService(new MetadataTransport(entities), "https://example.invalid", "test").Metadata;
        }

        public static IAttributeMetadataCache PersistentCache(string path, JsonRpc rpc, IOrganizationService service, Guid orgId)
        {
            // The connection cache types are internal to the signed language server assembly.
            var assembly = typeof(WebApiMetadataQuery).Assembly;
            var persistentType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Connection.PersistentMetadataCache", true);
            var persistent = Activator.CreateInstance(persistentType, path, rpc);
            var cacheType = assembly.GetType("MarkMpn.Sql4Cds.LanguageServer.Connection.CachedMetadata", true);
            return (IAttributeMetadataCache)Activator.CreateInstance(cacheType, service, persistent, orgId, "9.2", true);
        }

        internal sealed class MetadataTransport : IWebApiTransport
        {
            private readonly JObject[] _entities;
            public List<JObject> Queries { get; } = new List<JObject>();

            public MetadataTransport(JObject[] entities)
            {
                _entities = entities;
            }

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(requests.Select(request =>
                {
                    if (!request.Url.StartsWith("RetrieveMetadataChanges(", StringComparison.Ordinal))
                        throw new InvalidOperationException("Unexpected metadata request: " + request.Url);
                    var query = Query(request);
                    Queries.Add(query);
                    var condition = query["Criteria"]["Conditions"].Single();
                    var property = condition.Value<string>("PropertyName");
                    var value = condition["Value"].Value<string>("Value");
                    return Response(_entities.Where(e => String.Equals(e.Value<string>(property), value, StringComparison.OrdinalIgnoreCase)).ToArray());
                }).ToArray());
            }
        }
    }
}
