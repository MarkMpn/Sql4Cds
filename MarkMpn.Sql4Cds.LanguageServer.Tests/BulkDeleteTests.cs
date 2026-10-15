using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class BulkDeleteTests
    {
        private static PptbOrganizationService Service() => new PptbOrganizationService(new RejectTransport(), "https://example.invalid", "test");

        private static QueryExpression Query()
        {
            var query = new QueryExpression("account")
            {
                ColumnSet = new ColumnSet("accountid"),
                TopCount = 100,
                PageInfo = new PagingInfo { Count = 50, PageNumber = 2 },
                Criteria = new FilterExpression(LogicalOperator.Or)
            };
            query.Criteria.AddCondition("name", ConditionOperator.Equal, "O'Brien");
            query.Criteria.AddCondition("accountid", ConditionOperator.Equal, Guid.Parse("edbb9c62-cf72-49a9-81f8-9ebecb0a81c4"));
            query.Criteria.AddCondition("revenue", ConditionOperator.GreaterThan, 123.45m);
            query.Criteria.AddCondition("createdon", ConditionOperator.LessThan, new DateTime(2026, 10, 14, 0, 0, 0, DateTimeKind.Utc));
            query.Criteria.AddFilter(LogicalOperator.And).AddCondition("statecode", ConditionOperator.Equal, 0);
            var link = query.AddLink("contact", "primarycontactid", "contactid", JoinOperator.LeftOuter);
            link.EntityAlias = "contact";
            link.Columns.AddColumn("fullname");
            link.LinkCriteria.AddCondition("fullname", ConditionOperator.NotNull);
            query.AddOrder("name", OrderType.Descending);
            return query;
        }

        [TestMethod]
        public void BulkDeleteUsesQueryExpressionAndMapsTypedResponse()
        {
            var id = Guid.NewGuid();
            var start = new DateTime(2026, 10, 14, 0, 0, 0, DateTimeKind.Utc);
            var operation = Service().Prepare(new BulkDeleteRequest
            {
                QuerySet = new[] { Query() },
                JobName = "SQL bulk delete",
                StartDateTime = start,
                RecurrencePattern = "",
                SendEmailNotification = false,
                ToRecipients = Array.Empty<Guid>(),
                CCRecipients = Array.Empty<Guid>()
            });
            var request = operation.Requests.Single();
            Assert.AreEqual("POST", request.Method);
            Assert.AreEqual("BulkDelete", request.Url);
            var body = JObject.Parse(request.Body);
            Assert.AreEqual("SQL bulk delete", body.Value<string>("JobName"));
            Assert.AreEqual(false, body.Value<bool>("SendEmailNotification"));
            Assert.AreEqual("", body.Value<string>("RecurrencePattern"));
            Assert.AreEqual(start, body.Value<DateTime>("StartDateTime"));
            Assert.AreEqual(0, ((JArray)body["ToRecipients"]).Count);
            Assert.AreEqual(0, ((JArray)body["CCRecipients"]).Count);
            var query = body["QuerySet"][0];
            Assert.AreEqual("Microsoft.Dynamics.CRM.QueryExpression", query.Value<string>("@odata.type"));
            Assert.AreEqual("account", query.Value<string>("EntityName"));
            Assert.AreEqual("Or", query["Criteria"].Value<string>("FilterOperator"));
            Assert.AreEqual("System.String", query["Criteria"]["Conditions"][0]["Values"][0].Value<string>("Type"));
            Assert.AreEqual("O'Brien", query["Criteria"]["Conditions"][0]["Values"][0].Value<string>("Value"));
            Assert.AreEqual("System.Decimal", query["Criteria"]["Conditions"][2]["Values"][0].Value<string>("Type"));
            Assert.AreEqual("123.45", query["Criteria"]["Conditions"][2]["Values"][0].Value<string>("Value"));
            Assert.AreEqual("LeftOuter", query["LinkEntities"][0].Value<string>("JoinOperator"));
            Assert.IsNull(query["ExtensionData"]);
            Assert.AreEqual("Descending", query["Orders"][0].Value<string>("OrderType"));
            var response = (BulkDeleteResponse)operation.Map(new[] { Response(new JObject { ["JobId"] = id.ToString() }) });
            Assert.AreEqual(id, response.JobId);
        }

        [TestMethod]
        public void QueryConversionRoundTripsTypedConditionsLinksAndPaging()
        {
            var service = Service();
            var original = Query();
            var wire = service.SerializeParameter(original);
            var operation = service.Prepare(new FetchXmlToQueryExpressionRequest { FetchXml = "<fetch><entity name='account'/></fetch>" });
            Assert.AreEqual("GET", operation.Requests.Single().Method);
            StringAssert.StartsWith(operation.Requests.Single().Url, "FetchXmlToQueryExpression(");
            var response = (FetchXmlToQueryExpressionResponse)operation.Map(new[] { Response(new JObject { ["Query"] = wire }) });
            var query = response.Query;
            Assert.AreEqual(original.EntityName, query.EntityName);
            Assert.AreEqual(100, query.TopCount);
            Assert.AreEqual(2, query.PageInfo.PageNumber);
            Assert.AreEqual(LogicalOperator.Or, query.Criteria.FilterOperator);
            Assert.AreEqual(original.Criteria.Conditions[1].Values[0], query.Criteria.Conditions[1].Values[0]);
            Assert.AreEqual(123.45m, query.Criteria.Conditions[2].Values[0]);
            Assert.AreEqual(original.Criteria.Conditions[3].Values[0], query.Criteria.Conditions[3].Values[0]);
            Assert.AreEqual(0, query.Criteria.Filters[0].Conditions[0].Values[0]);
            Assert.AreEqual(JoinOperator.LeftOuter, query.LinkEntities[0].JoinOperator);
            Assert.AreEqual(ConditionOperator.NotNull, query.LinkEntities[0].LinkCriteria.Conditions[0].Operator);
            Assert.AreEqual("fullname", query.LinkEntities[0].Columns.Columns.Single());
            Assert.AreEqual(OrderType.Descending, query.Orders.Single().OrderType);

            var reverse = service.Prepare(new QueryExpressionToFetchXmlRequest { Query = query });
            Assert.AreEqual("POST", reverse.Requests.Single().Method);
            Assert.AreEqual("QueryExpressionToFetchXml", reverse.Requests.Single().Url);
            var converted = (QueryExpressionToFetchXmlResponse)reverse.Map(new[] { Response(new JObject { ["FetchXml"] = "<fetch/>" }) });
            Assert.AreEqual("<fetch/>", converted.FetchXml);
        }

        [TestMethod]
        public void NotificationRecipientsAreActivityParties()
        {
            var id = Guid.NewGuid();
            var operation = Service().Prepare(new BulkDeleteRequest { QuerySet = new[] { Query() }, ToRecipients = new[] { id }, CCRecipients = new[] { id } });
            var body = JObject.Parse(operation.Requests.Single().Body);
            Assert.AreEqual($"systemusers({id:D})", body["ToRecipients"][0].Value<string>("partyid_systemuser@odata.bind"));
            Assert.AreEqual($"systemusers({id:D})", body["CCRecipients"][0].Value<string>("partyid_systemuser@odata.bind"));
        }

        [TestMethod]
        public void FaultsAndInvalidResponsesAreNotReportedAsSuccessfulJobs()
        {
            var operation = Service().Prepare(new BulkDeleteRequest { QuerySet = new[] { Query() } });
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => operation.Map(new[]
            {
                new WebApiResponse { Status = 403, Body = "{\"error\":{\"message\":\"No bulk delete privilege\"}}" }
            }));
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { Response(new JObject()) }));
            Assert.ThrowsException<FormatException>(() => operation.Map(new[] { Response(new JObject { ["JobId"] = "bad-id" }) }));
        }

        [TestMethod]
        public void InvalidConversionResultsAndConditionTypesFailExplicitly()
        {
            var service = Service();
            var operation = service.Prepare(new FetchXmlToQueryExpressionRequest { FetchXml = "<fetch/>" });
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { Response(new JObject()) }));
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { Response(new JObject { ["Query"] = new JObject() }) }));
            var wire = service.SerializeParameter(Query());
            wire["Criteria"]["Conditions"][0]["Values"] = "invalid";
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { Response(new JObject { ["Query"] = wire }) }));
            wire = service.SerializeParameter(Query());
            wire["Criteria"]["Conditions"][0]["Values"][0]["Type"] = "Unexpected.Type";
            Assert.ThrowsException<NotSupportedException>(() => operation.Map(new[] { Response(new JObject { ["Query"] = wire }) }));
            var unsupported = new QueryExpression("account");
            unsupported.Criteria.AddCondition("name", ConditionOperator.Equal, new object());
            Assert.ThrowsException<NotSupportedException>(() => service.SerializeParameter(unsupported));
            var reverse = service.Prepare(new QueryExpressionToFetchXmlRequest { Query = Query() });
            Assert.ThrowsException<InvalidOperationException>(() => reverse.Map(new[] { Response(new JObject()) }));
        }

        [TestMethod]
        public void ExecuteUsesExistingTransportAndReturnsTypedJobResponse()
        {
            var transport = new RecordingTransport();
            var service = new PptbOrganizationService(transport, "https://example.invalid", "test");
            var converted = (FetchXmlToQueryExpressionResponse)service.Execute(new FetchXmlToQueryExpressionRequest
            {
                FetchXml = "<fetch><entity name='account'><filter><condition attribute='name' operator='eq' value='test'/></filter></entity></fetch>"
            });
            Assert.AreEqual("account", converted.Query.EntityName);
            Assert.AreEqual("test", converted.Query.Criteria.Conditions[0].Values[0]);
            var submitted = (BulkDeleteResponse)service.Execute(new BulkDeleteRequest
            {
                QuerySet = new[] { converted.Query },
                JobName = "test",
                StartDateTime = DateTime.UtcNow,
                RecurrencePattern = "",
                SendEmailNotification = false,
                ToRecipients = Array.Empty<Guid>(),
                CCRecipients = Array.Empty<Guid>()
            });
            Assert.AreEqual(transport.JobId, submitted.JobId);
            Assert.AreEqual(2, transport.Requests.Count);
            Assert.AreEqual("GET", transport.Requests[0].Method);
            Assert.AreEqual("POST", transport.Requests[1].Method);
            Assert.AreEqual("test", JObject.Parse(transport.Requests[1].Body)["QuerySet"][0]["Criteria"]["Conditions"][0]["Values"][0].Value<string>("Value"));
        }

        private sealed class RecordingTransport : IWebApiTransport
        {
            public readonly Guid JobId = Guid.NewGuid();
            public readonly List<WebApiRequest> Requests = new List<WebApiRequest>();

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                var request = requests.Single();
                Requests.Add(request);
                JObject body;
                if (request.Url.StartsWith("FetchXmlToQueryExpression(", StringComparison.Ordinal))
                {
                    body = JObject.Parse(@"{
                        'Query': {
                            '@odata.type': 'Microsoft.Dynamics.CRM.QueryExpression',
                            'EntityName': 'account',
                            'ColumnSet': { 'AllColumns': false, 'Columns': ['accountid'] },
                            'Criteria': {
                                'FilterOperator': 'And', 'Conditions': [
                                    { 'AttributeName': 'name', 'Operator': 'Equal', 'Values': [
                                        { 'Type': 'System.String', 'Value': 'test' }
                                    ] }
                                ], 'Filters': []
                            }, 'LinkEntities': [], 'Orders': []
                        }
                    }");
                }
                else
                {
                    Assert.AreEqual("BulkDelete", request.Url);
                    body = new JObject { ["JobId"] = JobId.ToString() };
                }
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(new[] { Response(body) });
            }
        }

        private static WebApiResponse Response(JObject body) => new WebApiResponse { Status = 200, Body = body.ToString() };

        private sealed class RejectTransport : IWebApiTransport
        {
            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
                => throw new InvalidOperationException("Tests must not submit real bulk delete jobs");
        }
    }
}
