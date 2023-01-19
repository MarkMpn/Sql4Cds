using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Loads the available transitions between different status codes
    /// </summary>
    static class StateTransitionLoader
    {
        public static Dictionary<int, StatusWithState> LoadStateTransitions(EntityMetadata meta)
        {
            if (meta.EnforceStateTransitions == true)
                return LoadCustomStateTransitions(meta);
            else
                return LoadWellKnownStateTransitions(meta);
        }

        private static Dictionary<int, StatusWithState> LoadCustomStateTransitions(EntityMetadata meta)
        {
            var graph = new Dictionary<int, StatusWithState>();

            // Add all the status codes in first
            var statusCodes = meta
                .Attributes
                .OfType<StatusAttributeMetadata>()
                .Single(a => a.LogicalName == "statuscode")
                .OptionSet
                .Options
                .OfType<StatusOptionMetadata>()
                .ToList();

            foreach (var statusCode in statusCodes)
                graph.Add(statusCode.Value.Value, new StatusWithState(statusCode));

            // Extract the allowed transitions from the metadata
            foreach (var statusCode in statusCodes.Where(sc => !String.IsNullOrEmpty(sc.TransitionData)))
            {
                var sourceStatusCode = graph[statusCode.Value.Value];

                var doc = XDocument.Parse(statusCode.TransitionData);
                var transitions = ((XElement)doc.FirstNode).Descendants();

                foreach (var transition in transitions)
                {
                    var targetStatusCodeValue = Int32.Parse(transition.Attribute("tostatusid").Value);

                    if (graph.TryGetValue(targetStatusCodeValue, out var targetStatusCode))
                        sourceStatusCode.Transitions[targetStatusCode] = StatusWithState.DefaultUpdateTransition;
                }
            }

            return graph;
        }

        private static Dictionary<int, StatusWithState> LoadWellKnownStateTransitions(EntityMetadata meta)
        {
            if (meta.LogicalName != "quote" && meta.LogicalName != "salesorder" && meta.LogicalName != "lead" && meta.LogicalName != "incident" && meta.LogicalName != "opportunity")
                return null;

            var graph = new Dictionary<int, StatusWithState>();

            // Add all the status codes in first
            var statusCodes = meta
                .Attributes
                .OfType<StatusAttributeMetadata>()
                .Single(a => a.LogicalName == "statuscode")
                .OptionSet
                .Options
                .OfType<StatusOptionMetadata>()
                .ToList();

            foreach (var statusCode in statusCodes)
                graph.Add(statusCode.Value.Value, new StatusWithState(statusCode));

            // Add the known state transitions
            if (meta.LogicalName == "quote")
            {
                // Can change from Draft to Active
                AddStateTransition(graph, statusCodes, 0, 1, StatusWithState.DefaultUpdateTransition);

                // Can change from Active to Won
                AddStateTransition(graph, statusCodes, 1, 2, WinQuoteTransition);

                // Can change from Active to Closed
                AddStateTransition(graph, statusCodes, 1, 3, CloseQuoteTransition);

                // Can change from anything back to draft (including one draft status code to another)
                foreach (var stateCode in new[] { 0, 1, 2, 3 })
                    AddStateTransition(graph, statusCodes, stateCode, 0, StatusWithState.DefaultUpdateTransition);
            }
            else if (meta.LogicalName == "salesorder")
            {
                // Can change from Active to Submitted
                AddStateTransition(graph, statusCodes, 0, 1, StatusWithState.DefaultUpdateTransition);

                // Can change from Active to Cancelled
                AddStateTransition(graph, statusCodes, 0, 2, CancelOrderTransition);

                // Can change from Active to Fulfilled
                AddStateTransition(graph, statusCodes, 0, 3, FulfillOrderTransition);

                // Can change from Active to Invoiced
                AddStateTransition(graph, statusCodes, 0, 4, StatusWithState.DefaultUpdateTransition);

                // Can change from anything back to active (including one active status code to another)
                foreach (var stateCode in new[] { 0, 1, 2, 3, 4 })
                    AddStateTransition(graph, statusCodes, stateCode, 0, StatusWithState.DefaultUpdateTransition);
            }
            else if (meta.LogicalName == "lead")
            {
                // Can change from Open to Qualified
                AddStateTransition(graph, statusCodes, 0, 1, StatusWithState.DefaultUpdateTransition);

                // Can change from Open to Disqualified
                AddStateTransition(graph, statusCodes, 0, 2, StatusWithState.DefaultUpdateTransition);

                // Can change from anything back to open (including one open status code to another)
                foreach (var stateCode in new[] { 0, 1, 2 })
                    AddStateTransition(graph, statusCodes, stateCode, 0, StatusWithState.DefaultUpdateTransition);
            }
            else if (meta.LogicalName == "incident")
            {
                // Can change from Active to Resolved
                AddStateTransition(graph, statusCodes, 0, 1, ResolveCaseTransition);

                // Can change from Active to Cancelled
                AddStateTransition(graph, statusCodes, 0, 2, StatusWithState.DefaultUpdateTransition);

                // Can change from anything back to active (including one active status code to another)
                foreach (var stateCode in new[] { 0, 1, 2 })
                    AddStateTransition(graph, statusCodes, stateCode, 0, StatusWithState.DefaultUpdateTransition);
            }
            else if (meta.LogicalName == "opportunity")
            {
                // Can change from Open to Won
                AddStateTransition(graph, statusCodes, 0, 1, WinOpportunityTransition);

                // Can change from Open to Lost
                AddStateTransition(graph, statusCodes, 0, 2, LoseOpportunityTransition);

                // Can change from anything back to open (including one open status code to another)
                foreach (var stateCode in new[] { 0, 1, 2 })
                    AddStateTransition(graph, statusCodes, stateCode, 0, StatusWithState.DefaultUpdateTransition);
            }
            else
            {
                return null;
            }

            return graph;
        }

        private static OrganizationRequest WinQuoteTransition(Entity entity, StatusWithState targetStatus)
        {
            return new WinQuoteRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                QuoteClose = new Entity("quoteclose")
                {
                    ["quoteid"] = entity.ToEntityReference()
                }
            };
        }

        private static OrganizationRequest CloseQuoteTransition(Entity entity, StatusWithState targetStatus)
        {
            return new CloseQuoteRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                QuoteClose = new Entity("quoteclose")
                {
                    ["quoteid"] = entity.ToEntityReference()
                }
            };
        }

        private static OrganizationRequest CancelOrderTransition(Entity entity, StatusWithState targetStatus)
        {
            return new CancelSalesOrderRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                OrderClose = new Entity("orderclose")
                {
                    ["salesorderid"] = entity.ToEntityReference()
                }
            };
        }

        private static OrganizationRequest FulfillOrderTransition(Entity entity, StatusWithState targetStatus)
        {
            return new FulfillSalesOrderRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                OrderClose = new Entity("orderclose")
                {
                    ["salesorderid"] = entity.ToEntityReference()
                }
            };
        }

        private static OrganizationRequest ResolveCaseTransition(Entity entity, StatusWithState targetStatus)
        {
            return new CloseIncidentRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                IncidentResolution = new Entity("incidentresolution")
                {
                    ["incidentid"] = entity.ToEntityReference()
                }
            };
        }

        private static OrganizationRequest WinOpportunityTransition(Entity entity, StatusWithState targetStatus)
        {
            return new WinOpportunityRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                OpportunityClose = new Entity("opportunityclose")
                {
                    ["opportunityid"] = entity.ToEntityReference()
                }
            };
        }

        private static OrganizationRequest LoseOpportunityTransition(Entity entity, StatusWithState targetStatus)
        {
            return new LoseOpportunityRequest
            {
                Status = new OptionSetValue(targetStatus.StatusCode),
                OpportunityClose = new Entity("opportunityclose")
                {
                    ["opportunityid"] = entity.ToEntityReference()
                }
            };
        }

        private static void AddStateTransition(Dictionary<int, StatusWithState> graph, List<StatusOptionMetadata> statusCodes, int fromStateCode, int toStateCode, Func<Entity, StatusWithState, OrganizationRequest> transition)
        {
            foreach (var fromStatusCodeMetadata in statusCodes.Where(s => s.State == fromStateCode))
            {
                var fromStatusCode = graph[fromStatusCodeMetadata.Value.Value];

                foreach (var toStatusCodeMetadata in statusCodes.Where(s => s.State == toStateCode))
                {
                    var toStatusCode = graph[toStatusCodeMetadata.Value.Value];
                    fromStatusCode.Transitions[toStatusCode] = transition;
                }
            }
        }
    }

    class StatusWithState
    {
        public StatusWithState(StatusOptionMetadata option)
        {
            StatusCode = option.Value.Value;
            StateCode = option.State.Value;
            Name = option.Label?.UserLocalizedLabel?.Label ?? option.Value.Value.ToString();
            Transitions = new Dictionary<StatusWithState, Func<Entity, StatusWithState, OrganizationRequest>>();
        }

        public int StatusCode { get; }

        public int StateCode { get; }

        public string Name { get; }

        public Dictionary<StatusWithState, Func<Entity, StatusWithState, OrganizationRequest>> Transitions { get; }

        public override int GetHashCode()
        {
            return StatusCode.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            return StatusCode.Equals(((StatusWithState)obj).StatusCode);
        }

        public static OrganizationRequest DefaultUpdateTransition(Entity entity, StatusWithState targetStatus)
        {
            return new UpdateRequest
            {
                Target = new Entity(entity.LogicalName, entity.Id)
                {
                    ["statecode"] = new OptionSetValue(targetStatus.StateCode),
                    ["statuscode"] = new OptionSetValue(targetStatus.StatusCode)
                }
            };
        }
    }
}
