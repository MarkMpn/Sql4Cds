using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.Threading;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine;
using MarkMpn.Sql4Cds.LanguageServer.Pptb;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Tests
{
    [TestClass]
    public class MessageMappingTests
    {
        private static MessageParameter Parameter(string name, Type type, string binding = null)
        {
            var parameter = new MessageParameter { Name = name, Type = type };
            parameter.SetBindingInformation(binding);
            return parameter;
        }

        private static PptbOrganizationService Service(params Message[] messages) =>
            new PptbOrganizationService(new MetadataTransport(), "https://example.invalid", "test")
            {
                MessageCache = new TestMessageCache(messages)
            };

        private static Message BoundMessage(string name, string entity, string parameter, params MessageParameter[] otherInputs) =>
            new Message
            {
                Name = name,
                BoundEntityLogicalName = entity,
                InputParameters = new[] { Parameter(parameter, typeof(Guid), "Bound:TRUE") }.Concat(otherInputs).ToArray(),
                OutputParameters = Array.Empty<MessageParameter>()
            };

        [TestMethod]
        public void BoundActionUsesMetadataAndOmitsBindingParameter()
        {
            var id = Guid.NewGuid();
            var message = BoundMessage("SendEmail", "email", "EmailId",
                Parameter("IssueSend", typeof(bool)), Parameter("TrackingToken", typeof(string)));
            var service = Service(message);
            var request = new SendEmailRequest { EmailId = id, IssueSend = true, TrackingToken = "test" };
            var operation = service.Prepare(request);
            var wire = operation.Requests.Single();
            Assert.AreEqual("POST", wire.Method);
            Assert.AreEqual($"emails({id:D})/Microsoft.Dynamics.CRM.SendEmail", wire.Url);
            var body = JObject.Parse(wire.Body);
            Assert.IsNull(body["EmailId"]);
            Assert.IsTrue(body.Value<bool>("IssueSend"));
            Assert.AreEqual("test", body.Value<string>("TrackingToken"));
            Assert.IsInstanceOfType<SendEmailResponse>(operation.Map(new[] { Response(new JObject { ["Subject"] = "test" }) }));
            request.EmailId = Guid.Empty;
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => service.Prepare(request));
        }

        [TestMethod]
        public void ExecuteUsesMetadataToSelectFunctionWithoutProbing()
        {
            var transport = new MetadataTransport();
            var message = BoundMessage("SendEmail", "email", "EmailId", Parameter("IssueSend", typeof(bool)));
            var service = new PptbOrganizationService(transport, "https://example.invalid", "test")
            {
                MessageCache = new TestMessageCache(new[] { message })
            };
            var id = Guid.NewGuid();
            Assert.IsInstanceOfType<SendEmailResponse>(service.Execute(new SendEmailRequest { EmailId = id, IssueSend = false }));
            Assert.AreEqual($"emails({id:D})/Microsoft.Dynamics.CRM.SendEmail", transport.Operations.Single().Url);
            message.Name = "CustomFunction";
            message.IsReadOnly = true;
            transport.Operations.Clear();
            service.MessageCache = new TestMessageCache(new[] { message });
            var request = new OrganizationRequest(message.Name);
            request["EmailId"] = id;
            request["IssueSend"] = false;
            var result = service.Execute(request);
            Assert.AreEqual("test", result["Result"]);
            Assert.AreEqual("GET", transport.Operations.Single().Method);
            StringAssert.StartsWith(transport.Operations.Single().Url, $"emails({id:D})/Microsoft.Dynamics.CRM.CustomFunction(IssueSend=@p1)");
            transport.Operations.Clear();
            service.Execute(request);
            Assert.AreEqual("GET", transport.Operations.Single().Method);
            message.IsReadOnly = false;
            transport.Operations.Clear();
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => service.Execute(request));
            Assert.AreEqual("POST", transport.Operations.Single().Method);
        }

        [TestMethod]
        public void EntityParameterArraysAreRenamedAndConverted()
        {
            var team = Guid.NewGuid();
            var member = Guid.NewGuid();
            var message = BoundMessage("AddMembersTeam", "team", "TeamId",
                Parameter("MemberIds", typeof(Guid[]), "OTC:8,AsEntityParameter:Members"));
            var service = Service(message);
            var request = new AddMembersTeamRequest { TeamId = team, MemberIds = new[] { member } };
            var wire = service.Prepare(request).Requests.Single();
            Assert.AreEqual($"teams({team:D})/Microsoft.Dynamics.CRM.AddMembersTeam", wire.Url);
            var body = JObject.Parse(wire.Body);
            Assert.IsNull(body["MemberIds"]);
            Assert.IsNull(body["TeamId"]);
            Assert.AreEqual("Microsoft.Dynamics.CRM.systemuser", body["Members"][0].Value<string>("@odata.type"));
            Assert.AreEqual(member.ToString("D"), body["Members"][0].Value<string>("systemuserid"));
            request.MemberIds = Array.Empty<Guid>();
            Assert.AreEqual(0, ((JArray)JObject.Parse(service.Prepare(request).Requests.Single().Body)["Members"]).Count);
        }

        [TestMethod]
        public void EntityResponsesAreMappedBackToSdkIds()
        {
            var workflow = Guid.NewGuid();
            var job = Guid.NewGuid();
            var message = BoundMessage("ExecuteWorkflow", "workflow", "WorkflowId", Parameter("EntityId", typeof(Guid)));
            message.OutputParameters = new[] { Parameter("Id", typeof(Guid), "OTC:4700,AsEntityParameter:AsyncOperation") };
            var operation = Service(message).Prepare(new ExecuteWorkflowRequest { WorkflowId = workflow, EntityId = Guid.NewGuid() });
            Assert.AreEqual($"workflows({workflow:D})/Microsoft.Dynamics.CRM.ExecuteWorkflow", operation.Requests.Single().Url);
            foreach (var body in new[]
            {
                new JObject { ["asyncoperationid"] = job.ToString(), ["name"] = "job" },
                new JObject { ["AsyncOperation"] = new JObject { ["asyncoperationid"] = job.ToString() } },
                new JObject { ["AsyncOperation"] = new JObject { ["@odata.id"] = $"asyncoperations({job:D})" } }
            })
            {
                var result = (ExecuteWorkflowResponse)operation.Map(new[] { Response(body) });
                Assert.AreEqual(job, result.Id);
                Assert.AreEqual(1, result.Results.Count);
            }
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { Response(new JObject()) }));
        }

        [TestMethod]
        public void FunctionsAndUnboundActionsApplyTheSameParameterMapping()
        {
            var id = Guid.NewGuid();
            var message = new Message
            {
                Name = "RetrieveUserPrivileges",
                IsReadOnly = true,
                BoundEntityLogicalName = "systemuser",
                InputParameters = new[] { Parameter("UserId", typeof(Guid), "Bound:TRUE"),
                    Parameter("OtherId", typeof(Guid), "OTC:8,AsEntityParameter:OtherUser") },
                OutputParameters = Array.Empty<MessageParameter>()
            };
            var request = new OrganizationRequest(message.Name);
            request["UserId"] = id;
            request["OtherId"] = id;
            var wire = Service(message).Prepare(request).Requests.Single();
            Assert.AreEqual("GET", wire.Method);
            StringAssert.StartsWith(wire.Url, $"systemusers({id:D})/Microsoft.Dynamics.CRM.RetrieveUserPrivileges(OtherUser=@p1)");
            StringAssert.Contains(Uri.UnescapeDataString(wire.Url), "\"systemuserid\":\"" + id.ToString("D") + "\"");
            message.Name = "CustomAction";
            message.IsReadOnly = false;
            message.BoundEntityLogicalName = null;
            request.RequestName = message.Name;
            wire = Service(message).Prepare(request).Requests.Single();
            Assert.AreEqual("CustomAction", wire.Url);
            Assert.IsNotNull(JObject.Parse(wire.Body)["UserId"]);
            Assert.IsNotNull(JObject.Parse(wire.Body)["OtherUser"]);
        }

        [DataTestMethod]
        [DataRow(true, "GET")]
        [DataRow(false, "POST")]
        public void UnboundMessagesUseMetadataRatherThanNames(bool readOnly, string method)
        {
            var message = new Message
            {
                Name = "RetrieveUserPrivileges",
                IsReadOnly = readOnly,
                InputParameters = Array.Empty<MessageParameter>(),
                OutputParameters = Array.Empty<MessageParameter>()
            };
            var wire = Service(message).Prepare(new OrganizationRequest(message.Name)).Requests.Single();
            Assert.AreEqual(method, wire.Method);
            Assert.AreEqual(readOnly ? "RetrieveUserPrivileges()" : "RetrieveUserPrivileges", wire.Url);
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => Service().Prepare(new OrganizationRequest("MissingMessage")));
        }

        [TestMethod]
        public void RenamedEntityResponsesPreserveSdkNamesAndCollectionTypes()
        {
            var id = Guid.NewGuid();
            var message = new Message
            {
                Name = "CustomMessage",
                InputParameters = Array.Empty<MessageParameter>(),
                OutputParameters = new[] { Parameter("Ids", typeof(Guid[]), "OTC:8,AsEntityParameter:Users"),
                    Parameter("Count", typeof(int)) }
            };
            var operation = Service(message).Prepare(new OrganizationRequest(message.Name));
            var result = operation.Map(new[] { Response(new JObject
            {
                ["Users"] = new JArray(new JObject { ["systemuserid"] = id.ToString() }), ["Count"] = 1
            }) });
            CollectionAssert.AreEqual(new[] { id }, (Guid[])result["Ids"]);
            Assert.IsFalse(result.Results.Contains("Users"));
            Assert.AreEqual(1, result["Count"]);
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { Response(new JObject { ["Count"] = 0 }) }));
            Assert.ThrowsException<InvalidOperationException>(() => operation.Map(new[] { new WebApiResponse { Status = 204 } }));
            message.OutputParameters = new[] { Parameter("Id", typeof(Guid), "AsEntityParameter:User") };
            Assert.ThrowsException<InvalidOperationException>(() => Service(message).Prepare(new OrganizationRequest(message.Name))
                .Map(new[] { Response(new JObject { ["User"] = new JObject { ["systemuserid"] = id.ToString() } }) }));
        }

        [TestMethod]
        public void BindingAcceptsEntityReferencesAndRejectsInvalidOrMissingTargets()
        {
            var id = Guid.NewGuid();
            var message = BoundMessage("CustomBound", "team", "Target");
            var service = Service(message);
            var request = new OrganizationRequest(message.Name);
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => service.Prepare(request));
            request["Target"] = new EntityReference("team", id);
            Assert.AreEqual($"teams({id:D})/Microsoft.Dynamics.CRM.CustomBound", service.Prepare(request).Requests.Single().Url);
            request["Target"] = new Entity("team", id);
            Assert.AreEqual($"teams({id:D})/Microsoft.Dynamics.CRM.CustomBound", service.Prepare(request).Requests.Single().Url);
            request["Target"] = new EntityReference("email", id);
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => service.Prepare(request));
            request["Target"] = "invalid";
            Assert.ThrowsException<FaultException<OrganizationServiceFault>>(() => service.Prepare(request));
            message.InputParameters = Array.Empty<MessageParameter>();
            Assert.ThrowsException<InvalidOperationException>(() => service.Prepare(request));
        }

        [TestMethod]
        public void ObjectTypeCodeResolutionIsCachedAndMissingEntitiesFail()
        {
            var calls = 0;
            var provider = new WebApiMetadataProvider(requests =>
            {
                calls++;
                return requests.Select(request => Response(request.Url.StartsWith("EntityDefinitions?", StringComparison.Ordinal) ?
                    new JObject { ["value"] = new JArray(new JObject { ["LogicalName"] = "systemuser" }) } :
                    new JObject { ["LogicalName"] = "systemuser", ["EntitySetName"] = "systemusers", ["PrimaryIdAttribute"] = "systemuserid" })).ToArray();
            });
            Assert.AreSame(provider.GetEntity(8), provider.GetEntity(8));
            Assert.AreEqual(2, calls);
            provider = new WebApiMetadataProvider(requests => new[] { Response(new JObject { ["value"] = new JArray() }) });
            Assert.ThrowsException<InvalidOperationException>(() => provider.GetEntity(8));
        }

        private static WebApiResponse Response(JObject json) => new WebApiResponse { Status = 200, Body = json.ToString() };

        private sealed class TestMessageCache : IMessageCache
        {
            private readonly Dictionary<string, Message> _messages;
            public TestMessageCache(IEnumerable<Message> messages) => _messages = messages.ToDictionary(m => m.Name, StringComparer.OrdinalIgnoreCase);
            public bool TryGetValue(string name, out Message message) => _messages.TryGetValue(name, out message);
            public IEnumerable<Message> GetAllMessages(bool lazy) => _messages.Values;
            public bool IsMessageAvailable(string entityLogicalName, string messageName) => _messages.ContainsKey(messageName);
        }

        private sealed class MetadataTransport : IWebApiTransport
        {
            public List<WebApiRequest> Operations { get; } = new List<WebApiRequest>();

            public Task<IReadOnlyList<WebApiResponse>> ExecuteBatchAsync(IReadOnlyList<WebApiRequest> requests, bool transaction, bool continueOnError, IReadOnlyDictionary<string, string> headers, CancellationToken cancellationToken)
            {
                return Task.FromResult<IReadOnlyList<WebApiResponse>>(requests.Select(request =>
                {
                    if (request.Url.StartsWith("EntityDefinitions?", StringComparison.Ordinal))
                        return Response(new JObject { ["value"] = new JArray(new JObject
                        {
                            ["LogicalName"] = request.Url.EndsWith("4700", StringComparison.Ordinal) ? "asyncoperation" : "systemuser"
                        }) });
                    if (!request.Url.StartsWith("EntityDefinitions(", StringComparison.Ordinal))
                    {
                        Operations.Add(request);
                        if (request.Url.Contains("CustomFunction") && request.Method == "POST")
                            return new WebApiResponse { Status = 405, Body = "{\"error\":{\"message\":\"Use GET\"}}" };
                        return Response(new JObject { ["Result"] = "test" });
                    }
                    Assert.IsTrue(request.Url.StartsWith("EntityDefinitions(", StringComparison.Ordinal));
                    var name = new[] { "email", "team", "workflow", "systemuser", "asyncoperation" }.Single(n => request.Url.Contains("'" + n + "'"));
                    return Response(new JObject { ["LogicalName"] = name, ["EntitySetName"] = name + "s", ["PrimaryIdAttribute"] = name + "id" });
                }).ToArray());
            }
        }
    }
}
