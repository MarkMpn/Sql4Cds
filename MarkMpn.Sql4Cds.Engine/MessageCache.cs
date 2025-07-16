﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Provides access to Dataverse messages and their input and output parameters
    /// </summary>
    public interface IMessageCache
    {
        /// <summary>
        /// Gets a message with a specific name
        /// </summary>
        /// <param name="name">The name of the message to get the details of</param>
        /// <param name="message">The details of the message with the requested <paramref name="name"/></param>
        /// <returns><c>true</c> if the message is found in the cache, or <c>false</c> otherwise</returns>
        bool TryGetValue(string name, out Message message);

        /// <summary>
        /// Gets a list of all messages in the instance
        /// </summary>
        /// <param name="lazy"><see langword="true"/> to only return message details that are immediately available, or <see langword="false"/> to force all messages to be retrieved</param>
        /// <returns>A list of all messages in the instance</returns>
        IEnumerable<Message> GetAllMessages(bool lazy);

        /// <summary>
        /// Indicates whether a specific message is valid for an entity
        /// </summary>
        /// <param name="entityLogicalName">The logical name of the entity</param>
        /// <param name="messageName">The name of the message</param>
        /// <returns><c>true</c> if the message is available for this entity, or <c>false</c> otherwise</returns>
        bool IsMessageAvailable(string entityLogicalName, string messageName);
    }

    /// <summary>
    /// Provides methods to access the list of messages available for use in SQL scripts
    /// </summary>
    public class MessageCache : IMessageCache
    {
        private readonly IOrganizationService _org;
        private readonly IAttributeMetadataCache _metadata;
        private readonly object _loaderLock;
        private Dictionary<string, Message> _cache;
        private Dictionary<string, bool> _entityMessages;

        /// <summary>
        /// Loads a list of messages from a specific instance
        /// </summary>
        /// <param name="org">A connection to the instance to load the messages from</param>
        /// <param name="metadata">A cache of metadata for this connection</param>
        public MessageCache(IOrganizationService org, IAttributeMetadataCache metadata)
        {
            _org = org;
            _metadata = metadata;
            _loaderLock = new object();
        }

        private void Load()
        {
            lock (_loaderLock)
            {
                if (_cache != null)
                    return;

                _entityMessages = new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

                // Load the requests and input parameters
                var requestQry = new QueryExpression("sdkmessagerequest");
                requestQry.ColumnSet = new ColumnSet("name");
                var messagePairLink = requestQry.AddLink("sdkmessagepair", "sdkmessagepairid", "sdkmessagepairid");
                messagePairLink.LinkCriteria.AddCondition("endpoint", ConditionOperator.Equal, "api/data");
                var fieldLink = requestQry.AddLink("sdkmessagerequestfield", "sdkmessagerequestid", "sdkmessagerequestid", JoinOperator.LeftOuter);
                fieldLink.EntityAlias = "sdkmessagerequestfield";
                fieldLink.Columns = new ColumnSet("name", "clrparser", "position", "optional", "parameterbindinginformation");

                var messageRequestFields = new Dictionary<string, List<MessageParameter>>();

                foreach (var entity in RetrieveAll(requestQry))
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
                    var fieldOptional = entity.GetAttributeValue<AliasedValue>("sdkmessagerequestfield.optional");
                    var fieldBindingInfo = entity.GetAttributeValue<AliasedValue>("sdkmessagerequestfield.parameterbindinginformation");

                    if (fieldName != null)
                    {
                        requestFields.Add(new MessageParameter
                        {
                            Name = (string)fieldName.Value,
                            Type = fieldType == null ? null : GetType((string)fieldType.Value),
                            Position = (int)fieldPosition.Value,
                            Optional = (bool)(fieldOptional?.Value ?? false),
                            OTC = fieldBindingInfo == null ? null : ExtractOTC((string)fieldBindingInfo.Value)
                        });
                    }
                }

                // Load the response parameters
                var responseQry = new QueryExpression("sdkmessageresponse");
                var requestLink = responseQry.AddLink("sdkmessagerequest", "sdkmessagerequestid", "sdkmessagerequestid");
                requestLink.EntityAlias = "sdkmessagerequest";
                requestLink.Columns = new ColumnSet("name");
                messagePairLink = requestLink.AddLink("sdkmessagepair", "sdkmessagepairid", "sdkmessagepairid");
                messagePairLink.LinkCriteria.AddCondition("endpoint", ConditionOperator.Equal, "api/data");
                fieldLink = responseQry.AddLink("sdkmessageresponsefield", "sdkmessageresponseid", "sdkmessageresponseid");
                fieldLink.EntityAlias = "sdkmessageresponsefield";
                fieldLink.Columns = new ColumnSet("name", "clrformatter", "position", "parameterbindinginformation");

                var messageResponseFields = new Dictionary<string, List<MessageParameter>>();

                foreach (var entity in RetrieveAll(responseQry))
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
                        OutputParameters = (IReadOnlyList<MessageParameter>)responseFields?.AsReadOnly() ?? Array.Empty<MessageParameter>()
                    };
                }, StringComparer.OrdinalIgnoreCase);
            }
        }

        private IEnumerable<Entity> RetrieveAll(QueryExpression qry)
        {
            RemoveMissingAttributes(qry);

            var results = _org.RetrieveMultiple(qry);

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

                results = _org.RetrieveMultiple(qry);

                foreach (var entity in results.Entities)
                    yield return entity;
            }
        }

        private void RemoveMissingAttributes(QueryExpression qry)
        {
            var logicalName = qry.EntityName;
            var columnSet = qry.ColumnSet;
            RemoveMissingAttributes(_metadata[logicalName], columnSet);

            foreach (var linkEntity in qry.LinkEntities)
                RemoveMissingAttributes(linkEntity, _metadata);
        }

        private void RemoveMissingAttributes(LinkEntity linkEntity, IAttributeMetadataCache metadata)
        {
            var logicalName = linkEntity.LinkToEntityName;
            var columnSet = linkEntity.Columns;
            RemoveMissingAttributes(metadata[logicalName], columnSet);

            foreach (var childLink in linkEntity.LinkEntities)
                RemoveMissingAttributes(childLink, metadata);
        }

        private void RemoveMissingAttributes(EntityMetadata metadata, ColumnSet columns)
        {
            for (var i = columns.Columns.Count - 1; i >= 0; i--)
            {
                if (!metadata.Attributes.Any(a => a.LogicalName == columns.Columns[i]))
                    columns.Columns.RemoveAt(i);
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
            catch
            {
                return null;
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

        public bool TryGetValue(string name, out Message message)
        {
            Load();
            return _cache.TryGetValue(name, out message);
        }

        public IEnumerable<Message> GetAllMessages(bool lazy)
        {
            if (lazy && _cache == null)
            {
                _ = Task.Run(() => Load());
                return Enumerable.Empty<Message>();
            }

            Load();
            return _cache.Values;
        }

        public bool IsMessageAvailable(string entityLogicalName, string messageName)
        {
            Load();
            var key = entityLogicalName + ":" + messageName;

            if (_entityMessages.TryGetValue(key, out var value))
                return value;

            // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/org-service/use-createmultiple-updatemultiple?tabs=sdk#limited-to-certain-standard-tables
            var query = new QueryExpression("sdkmessagefilter")
            {
                ColumnSet = new ColumnSet("sdkmessagefilterid"),
                Criteria = new FilterExpression(LogicalOperator.And)
                {
                    Conditions =
                    {
                        new ConditionExpression(
                            attributeName:"primaryobjecttypecode",
                            conditionOperator: ConditionOperator.Equal,
                            value: entityLogicalName)
                    }
                },
                LinkEntities =
                {
                    new LinkEntity(
                        linkFromEntityName:"sdkmessagefilter",
                        linkToEntityName:"sdkmessage",
                        linkFromAttributeName:"sdkmessageid",
                        linkToAttributeName:"sdkmessageid",
                        joinOperator: JoinOperator.Inner)
                    {
                        LinkCriteria = new FilterExpression(LogicalOperator.And)
                        {
                            Conditions =
                            {
                                new ConditionExpression(
                                    attributeName:"name",
                                    conditionOperator: ConditionOperator.Equal,
                                    value: messageName)
                            }
                        }
                    }
                }
            };

            var entityCollection = _org.RetrieveMultiple(query);
            value = entityCollection.Entities.Count > 0;

            _entityMessages[key] = value;
            return value;
        }
    }

    /// <summary>
    /// Describes the metadata for a message
    /// </summary>
    public class Message
    {
        /// <summary>
        /// Returns or sets the name of the message
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Returns or sets the list of input parameters for the message
        /// </summary>
        public IReadOnlyList<MessageParameter> InputParameters { get; set; }

        /// <summary>
        /// Returns or sets the list of output parameters for the message
        /// </summary>
        public IReadOnlyList<MessageParameter> OutputParameters { get; set; }

        /// <summary>
        /// Checks if the message can be called as a table valued function in SQL queries
        /// </summary>
        /// <returns><c>true</c> if the message is allowed to be called as a table valued function, or <c>false</c> otherwise</returns>
        public bool IsValidAsTableValuedFunction()
        {
            return IsValid(true);
        }

        /// <summary>
        /// Checks if the message can be called as a stored procedure in SQL queries
        /// </summary>
        /// <returns><c>true</c> if the message is allowed to be called as a stored procedure, or <c>false</c> otherwise</returns>
        public bool IsValidAsStoredProcedure()
        {
            return IsValid(false);
        }

        private bool IsValid(bool requireOutput)
        {
            // 1. Any request fields must be scalar values (except for a single PagingInfo parameter if the output is an entity collection)
            // 2. The response fields must be EITHER:
            //    a. A single field of an entity-derived type, OR
            //    b. A single field of an entity collection, OR
            //    c. A single field of an array of a scalar type, OR
            //    d. one or more fields of a scalar type

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
                return !requireOutput;

            if (OutputParameters.All(p => p.IsScalarType()))
                return true;

            if (OutputParameters.Count > 1)
                return false;

            if (OutputParameters[0].Type == typeof(Entity) || OutputParameters[0].Type == typeof(AuditDetail))
                return true;

            if (OutputParameters[0].Type == typeof(EntityCollection) || OutputParameters[0].Type == typeof(AuditDetailCollection))
                return true;

            if (OutputParameters[0].Type.IsArray && MessageParameter.IsScalarType(OutputParameters[0].Type.GetElementType()))
                return true;

            return false;
        }
    }

    /// <summary>
    /// Describes an input or output parameter of a <see cref="Message"/>
    /// </summary>
    public class MessageParameter
    {
        /// <summary>
        /// Returns or sets the name of the parameter
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Returns or sets the type of the parameter
        /// </summary>
        public Type Type { get; set; }

        /// <summary>
        /// Returns or sets the ordinal position of the parameter
        /// </summary>
        public int Position { get; set; }

        /// <summary>
        /// Returns or sets the object type code of the entity or entity collection used by the parameter
        /// </summary>
        public int? OTC { get; set; }

        /// <summary>
        /// Indicates whether the input parameter is optional
        /// </summary>
        public bool Optional { get; set; }

        /// <summary>
        /// Checks if the parameter is a scalar type
        /// </summary>
        /// <returns><c>true</c> if the <see cref="Type"/> is a scalar type</returns>
        public bool IsScalarType()
        {
            return IsScalarType(Type);
        }

        internal static bool IsScalarType(Type type)
        {
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>))
                type = type.GetGenericArguments()[0];

            if (type == typeof(string) || type == typeof(Guid) || type == typeof(bool) || type == typeof(int) ||
                type == typeof(EntityReference) || type == typeof(DateTime) || type == typeof(long) || type == typeof(OptionSetValue) ||
                type == typeof(Money))
                return true;

            // Entity values can be converted to JSON strings and so can be treated as scalar values
            if (type == typeof(Entity))
                return true;

            return false;
        }

        /// <summary>
        /// Returns the SQL data type for the parameter
        /// </summary>
        public Microsoft.SqlServer.TransactSql.ScriptDom.DataTypeReference GetSqlDataType(DataSource dataSource)
        {
            if (Type == typeof(Entity))
                return DataTypeHelpers.NVarChar(Int32.MaxValue, dataSource?.DefaultCollation ?? Collation.USEnglish, CollationLabel.Implicit);

            if (Type == typeof(EntityReference) && OTC != null)
            {
                var logicalName = dataSource.Metadata[OTC.Value].LogicalName;
                return DataTypeHelpers.TypedEntityReference(logicalName);
            }

            return SqlTypeConverter.NetToSqlType(Type).ToSqlType(dataSource);
        }
    }
}
