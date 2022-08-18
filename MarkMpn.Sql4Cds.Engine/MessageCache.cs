using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    public interface IMessageCache
    {
        Message this[string name] { get; }

        bool TryGetValue(string name, out Message message);

        IEnumerable<Message> GetAllMessages();
    }

    /// <summary>
    /// Provides methods to access the list of messages available for use in SQL scripts
    /// </summary>
    public class MessageCache : IMessageCache
    {
        private readonly Dictionary<string, Message> _cache;

        public MessageCache(IOrganizationService org)
        {
            // Load the requests and input parameters
            var requestQry = new QueryExpression("sdkmessagerequest");
            requestQry.ColumnSet = new ColumnSet("name");
            var messagePairLink = requestQry.AddLink("sdkmessagepair", "sdkmessagepairid", "sdkmessagepairid");
            messagePairLink.LinkCriteria.AddCondition("endpoint", ConditionOperator.Equal, "2011/Organization.svc");
            var fieldLink = requestQry.AddLink("sdkmessagerequestfield", "sdkmessagerequestid", "sdkmessagerequestid", JoinOperator.LeftOuter);
            fieldLink.EntityAlias = "sdkmessagerequestfield";
            fieldLink.Columns = new ColumnSet("name", "clrparser", "position");

            var messageRequestFields = new Dictionary<string, List<MessageParameter>>();

            foreach (var entity in RetrieveAll(org, requestQry))
            {
                var messageName = entity.GetAttributeValue<string>("name");

                if (!messageRequestFields.TryGetValue(messageName, out var requestFields))
                {
                    requestFields = new List<MessageParameter>();
                    messageRequestFields.Add(messageName, requestFields);
                }

                var fieldName = entity.GetAttributeValue<AliasedValue>("sdkmessagerequestfield.name");
                var fieldType = entity.GetAttributeValue<AliasedValue>("sdkmessagerequestfield.clrparser");
                var fieldPosition = entity.GetAttributeValue<AliasedValue>("sdkmessagerequestfield.position");

                if (fieldName != null)
                {
                    requestFields.Add(new MessageParameter
                    {
                        Name = (string)fieldName.Value,
                        Type = fieldType == null ? null : GetType((string)fieldType.Value),
                        Position = (int)fieldPosition.Value
                    });
                }
            }

            // Load the response parameters
            var responseQry = new QueryExpression("sdkmessageresponse");
            var requestLink = responseQry.AddLink("sdkmessagerequest", "sdkmessagerequestid", "sdkmessagerequestid");
            requestLink.EntityAlias = "sdkmessagerequest";
            requestLink.Columns = new ColumnSet("name");
            messagePairLink = requestLink.AddLink("sdkmessagepair", "sdkmessagepairid", "sdkmessagepairid");
            messagePairLink.LinkCriteria.AddCondition("endpoint", ConditionOperator.Equal, "2011/Organization.svc");
            fieldLink = responseQry.AddLink("sdkmessageresponsefield", "sdkmessageresponseid", "sdkmessageresponseid");
            fieldLink.EntityAlias = "sdkmessageresponsefield";
            fieldLink.Columns = new ColumnSet("name", "clrformatter", "position", "parameterbindinginformation");

            var messageResponseFields = new Dictionary<string, List<MessageParameter>>();

            foreach (var entity in RetrieveAll(org, responseQry))
            {
                var messageName = (string)entity.GetAttributeValue<AliasedValue>("sdkmessagerequest.name").Value;

                if (!messageResponseFields.TryGetValue(messageName, out var responseFields))
                {
                    responseFields = new List<MessageParameter>();
                    messageResponseFields.Add(messageName, responseFields);
                }

                var fieldName = entity.GetAttributeValue<AliasedValue>("sdkmessageresponsefield.name");
                var fieldType = entity.GetAttributeValue<AliasedValue>("sdkmessageresponsefield.clrformatter");
                var fieldPosition = entity.GetAttributeValue<AliasedValue>("sdkmessageresponsefield.position");
                var fieldBindingInfo = entity.GetAttributeValue<AliasedValue>("sdkmessageresponsefield.parameterbindinginformation");

                responseFields.Add(new MessageParameter
                {
                    Name = (string)fieldName.Value,
                    Type = fieldType == null ? null : GetType((string)fieldType.Value),
                    Position = (int)fieldPosition.Value,
                    OTC = fieldBindingInfo == null ? null : ExtractOTC((string)fieldBindingInfo.Value)
                });
            }

            _cache = messageRequestFields.ToDictionary(kvp => kvp.Key, kvp =>
            {
                var requestFields = kvp.Value;
                requestFields.Sort((x, y) => x.Position.CompareTo(y.Position));
                messageResponseFields.TryGetValue(kvp.Key, out var responseFields);

                return new Message
                {
                    Name = kvp.Key,
                    InputParameters = requestFields.AsReadOnly(),
                    OutputParameters = (IReadOnlyList<MessageParameter>) responseFields?.AsReadOnly() ?? Array.Empty<MessageParameter>()
                };
            }, StringComparer.OrdinalIgnoreCase);
        }

        private IEnumerable<Entity> RetrieveAll(IOrganizationService org, QueryExpression qry)
        {
            var results = org.RetrieveMultiple(qry);

            foreach (var entity in results.Entities)
                yield return entity;

            var pageNumber = 1;

            while (results.MoreRecords)
            {
                pageNumber++;
                qry.PageInfo = new PagingInfo
                {
                    PageNumber = pageNumber,
                    Count = results.Entities.Count,
                    PagingCookie = results.PagingCookie
                };

                results = org.RetrieveMultiple(qry);

                foreach (var entity in results.Entities)
                    yield return entity;
            }
        }

        private Type GetType(string typeName)
        {
            if (String.IsNullOrEmpty(typeName))
                return null;

            try
            {
                return Type.GetType(typeName);
            }
            catch (Exception ex)
            {
                throw new ApplicationException("Error retrieving type " + typeName + ":" + ex.Message, ex);
            }
        }

        private int? ExtractOTC(string fieldBindingInfo)
        {
            if (String.IsNullOrEmpty(fieldBindingInfo))
                return null;

            if (!fieldBindingInfo.StartsWith("OTC:"))
                return null;

            if (!Int32.TryParse(fieldBindingInfo.Substring(4), out var otc))
                return null;

            return otc;
        }

        public Message this[string name]
        {
            get
            {
                return _cache[name];
            }
        }

        public bool TryGetValue(string name, out Message message)
        {
            return _cache.TryGetValue(name, out message);
        }

        public IEnumerable<Message> GetAllMessages()
        {
            return _cache.Values;
        }
    }

    public class Message
    {
        public string Name { get; set; }

        public IReadOnlyList<MessageParameter> InputParameters { get; set; }

        public IReadOnlyList<MessageParameter> OutputParameters { get; set; }

        public bool IsValidAsTableValuedFunction()
        {
            // 1. Any request fields must be scalar values (except for a single PagingInfo parameter if the output is an entity collection)
            // 2. The response fields must be EITHER:
            //    a. A single field of an entity-derived type, OR
            //    a. A single field of an entity collection, OR
            //    b. one or more fields of a scalar type

            if (InputParameters.Any(p => p.Type == null))
                return false;

            if (OutputParameters.Any(p => p.Type == null))
                return false;

            if (OutputParameters.Count == 1 && (OutputParameters[0].Type == typeof(EntityCollection) || OutputParameters[0].Type == typeof(AuditDetailCollection)))
            {
                var pagingInfoInputs = InputParameters.Where(p => p.Type == typeof(PagingInfo)).ToList();

                if (pagingInfoInputs.Count > 1)
                    return false;

                if (InputParameters.Any(p => !p.IsScalarType() && p.Type != typeof(PagingInfo)))
                    return false;
            }
            else
            {
                if (InputParameters.Any(p => !p.IsScalarType()))
                    return false;
            }

            if (OutputParameters.Count == 0)
                return false;

            if (OutputParameters.All(p => p.IsScalarType()))
                return true;

            if (OutputParameters.Count > 1)
                return false;

            if ((OutputParameters[0].Type == typeof(Entity) && OutputParameters[0].OTC != null) || OutputParameters[0].Type == typeof(AuditDetail))
                return true;

            if ((OutputParameters[0].Type == typeof(EntityCollection) && OutputParameters[0].OTC != null) || OutputParameters[0].Type == typeof(AuditDetailCollection))
                return true;

            return false;
        }
    }

    public class MessageParameter
    {
        public string Name { get; set; }
        public Type Type { get; set; }
        public int Position { get; set; }
        public int? OTC { get; set; }

        public bool IsScalarType()
        {
            var type = Type;

            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type == typeof(string) || type == typeof(Guid) || type == typeof(bool) || type == typeof(int) ||
                type == typeof(EntityReference) || type == typeof(DateTime) || type == typeof(long) || type == typeof(OptionSetValue) ||
                type == typeof(Money))
                return true;

            return false;
        }
    }
}
