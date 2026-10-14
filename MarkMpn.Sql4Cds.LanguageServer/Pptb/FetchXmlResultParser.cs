using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Xml.Linq;
using Microsoft.Xrm.Sdk;
using Newtonsoft.Json.Linq;

namespace MarkMpn.Sql4Cds.LanguageServer.Pptb
{
    /// <summary>
    /// Converts the results of a FetchXML query executed through the Web API back into the format that
    /// would have been returned by the SDK
    /// </summary>
    public class FetchXmlResultParser
    {
        public const string PagingCookieAnnotation = "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie";
        public const string MoreRecordsAnnotation = "@Microsoft.Dynamics.CRM.morerecords";
        public const string TotalRecordCountAnnotation = "@Microsoft.Dynamics.CRM.totalrecordcount";
        public const string TotalRecordCountLimitExceededAnnotation = "@Microsoft.Dynamics.CRM.totalrecordcountlimitexceeded";

        class AttributeDetails
        {
            public string EntityName { get; set; }
            public string AttributeName { get; set; }
            public string Aggregate { get; set; }
            public string DateGrouping { get; set; }
            public bool IsLinkEntity { get; set; }
        }

        private readonly WebApiMetadataProvider _metadata;
        private readonly string _rootEntity;
        private readonly Dictionary<string, AttributeDetails> _attributeAliases = new Dictionary<string, AttributeDetails>(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _linkAliases = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _linkEntityNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        public FetchXmlResultParser(string fetchXml, WebApiMetadataProvider metadata)
        {
            _metadata = metadata;

            var doc = XDocument.Parse(fetchXml);
            var entity = doc.Root?.Element("entity") ?? throw new ArgumentException("FetchXML does not contain an entity element");
            _rootEntity = (string)entity.Attribute("name");
            Process(entity, _rootEntity, false);
        }

        public string RootEntity => _rootEntity;

        private void Process(XElement element, string entityName, bool isLinkEntity)
        {
            foreach (var child in element.Elements())
            {
                if (child.Name.LocalName == "attribute")
                {
                    var alias = (string)child.Attribute("alias");

                    if (!String.IsNullOrEmpty(alias))
                    {
                        _attributeAliases[alias] = new AttributeDetails
                        {
                            EntityName = entityName,
                            AttributeName = (string)child.Attribute("name"),
                            Aggregate = (string)child.Attribute("aggregate"),
                            DateGrouping = (string)child.Attribute("dategrouping"),
                            IsLinkEntity = isLinkEntity
                        };
                    }
                }
                else if (child.Name.LocalName == "link-entity")
                {
                    var name = (string)child.Attribute("name");
                    var alias = (string)child.Attribute("alias");

                    if (!String.IsNullOrEmpty(alias))
                        _linkAliases[alias] = name;

                    _linkEntityNames.Add(name);
                    Process(child, name, true);
                }
                else if (child.Name.LocalName == "filter")
                {
                    // Filters can contain link-entity elements for any/all/not any/not all queries, but these
                    // can't return any attributes so we can skip them
                }
            }
        }

        /// <summary>
        /// Converts the response to the FetchXML query to an <see cref="EntityCollection"/>
        /// </summary>
        public EntityCollection Parse(JObject response)
        {
            var result = new EntityCollection { EntityName = _rootEntity };

            if (response["value"] is JArray records)
            {
                foreach (var record in records.OfType<JObject>())
                    result.Entities.Add(ParseEntity(record));
            }

            var cookie = response.Value<string>(PagingCookieAnnotation);

            if (cookie != null)
                result.PagingCookie = ExtractPagingCookie(cookie);

            if (response[MoreRecordsAnnotation] is JValue moreRecords && moreRecords.Type == JTokenType.Boolean)
                result.MoreRecords = (bool)moreRecords;

            if (response[TotalRecordCountAnnotation] is JValue totalRecordCount && totalRecordCount.Type == JTokenType.Integer)
                result.TotalRecordCount = (int)totalRecordCount;

            if (response[TotalRecordCountLimitExceededAnnotation] is JValue limitExceeded && limitExceeded.Type == JTokenType.Boolean)
                result.TotalRecordCountLimitExceeded = (bool)limitExceeded;

            return result;
        }

        /// <summary>
        /// Extracts the SDK-style paging cookie from the Web API paging cookie annotation
        /// </summary>
        public static string ExtractPagingCookie(string annotation)
        {
            try
            {
                var cookieElement = XElement.Parse(annotation);
                var pagingCookie = (string)cookieElement.Attribute("pagingcookie");

                if (pagingCookie == null)
                    return null;

                return WebUtility.UrlDecode(WebUtility.UrlDecode(pagingCookie));
            }
            catch
            {
                return null;
            }
        }

        private Entity ParseEntity(JObject record)
        {
            var entity = new Entity(_rootEntity);
            var rootInfo = _metadata.GetEntity(_rootEntity);

            foreach (var prop in record.Properties())
            {
                if (prop.Name.Contains('@'))
                    continue;

                var key = prop.Name;
                var formattedValue = record.Value<string>(prop.Name + WebApiJson.FormattedValueAnnotation);
                var lookupLogicalName = record.Value<string>(prop.Name + WebApiJson.LookupLogicalNameAnnotation);
                string entityName;
                string attributeName;
                AttributeDetails aliasDetails = null;

                if (_attributeAliases.TryGetValue(key, out aliasDetails))
                {
                    entityName = aliasDetails.EntityName;
                    attributeName = aliasDetails.AttributeName;
                }
                else if (key.Contains('.'))
                {
                    var dot = key.IndexOf('.');
                    var linkAlias = key.Substring(0, dot);
                    attributeName = StripLookupSuffix(key.Substring(dot + 1));
                    entityName = ResolveLinkEntity(linkAlias);
                    key = linkAlias + "." + attributeName;
                }
                else
                {
                    entityName = _rootEntity;
                    attributeName = StripLookupSuffix(key);
                    key = attributeName;
                }

                var entityInfo = entityName == null ? null : TryGetEntity(entityName);
                var attrInfo = entityInfo?.GetAttribute(attributeName);

                object value;

                if (aliasDetails?.Aggregate != null || aliasDetails?.DateGrouping != null)
                    value = ConvertAggregate(prop.Value, aliasDetails, attrInfo, lookupLogicalName, formattedValue);
                else
                    value = ConvertValue(prop.Value, attrInfo, lookupLogicalName, formattedValue);

                var isAliased = aliasDetails != null || entityName != _rootEntity || key.Contains('.');

                if (isAliased)
                    entity[key] = new AliasedValue(entityName, attributeName, value);
                else
                    entity[key] = value;

                if (formattedValue != null && !(value is EntityReference))
                    entity.FormattedValues[key] = formattedValue;

                if (!isAliased && rootInfo?.PrimaryIdAttribute != null && attributeName.Equals(rootInfo.PrimaryIdAttribute, StringComparison.OrdinalIgnoreCase) && value is Guid id)
                    entity.Id = id;
            }

            return entity;
        }

        private WebApiEntityInfo TryGetEntity(string logicalName)
        {
            try
            {
                return _metadata.GetEntity(logicalName);
            }
            catch
            {
                return null;
            }
        }

        private string ResolveLinkEntity(string alias)
        {
            if (_linkAliases.TryGetValue(alias, out var entityName))
                return entityName;

            // Link entities without an explicit alias are given an automatic alias of the entity name followed by a number
            var trimmed = alias.TrimEnd('0', '1', '2', '3', '4', '5', '6', '7', '8', '9');

            if (_linkEntityNames.Contains(trimmed))
                return trimmed;

            return trimmed;
        }

        private static string StripLookupSuffix(string attributeName)
        {
            if (attributeName.StartsWith("_") && attributeName.EndsWith("_value") && attributeName.Length > 7)
                return attributeName.Substring(1, attributeName.Length - 7);

            return attributeName;
        }

        private static object ConvertAggregate(JToken token, AttributeDetails details, WebApiAttributeInfo attrInfo, string lookupLogicalName, string formattedValue)
        {
            if (token.Type == JTokenType.Null)
                return null;

            if (details.DateGrouping != null)
                return ToInt(token);

            switch (details.Aggregate?.ToLowerInvariant())
            {
                case "count":
                case "countcolumn":
                    return ToInt(token);

                case "avg":
                    switch (attrInfo?.AttributeType)
                    {
                        case "Money":
                            return new Money(token.Value<decimal>());

                        case "Double":
                            return token.Value<double>();

                        default:
                            return token.Value<decimal>();
                    }

                default:
                    return ConvertValue(token, attrInfo, lookupLogicalName, formattedValue);
            }
        }

        private static object ToInt(JToken token)
        {
            if (token.Type == JTokenType.String && Int32.TryParse((string)token, NumberStyles.Integer, CultureInfo.InvariantCulture, out var i))
                return i;

            var l = token.Value<long>();

            if (l >= Int32.MinValue && l <= Int32.MaxValue)
                return (int)l;

            return l;
        }

        /// <summary>
        /// Converts a JSON value to the SDK type for an attribute
        /// </summary>
        public static object ConvertValue(JToken token, WebApiAttributeInfo attrInfo, string lookupLogicalName, string formattedValue)
        {
            if (token == null || token.Type == JTokenType.Null)
                return null;

            if (lookupLogicalName != null && token.Type == JTokenType.String && Guid.TryParse((string)token, out var lookupId))
                return new EntityReference(lookupLogicalName, lookupId) { Name = formattedValue };

            switch (attrInfo?.AttributeType)
            {
                case "Lookup":
                case "Customer":
                case "Owner":
                    if (Guid.TryParse((string)token, out var refId))
                        return new EntityReference(lookupLogicalName, refId) { Name = formattedValue };
                    break;

                case "Picklist":
                case "State":
                case "Status":
                    return new OptionSetValue(token.Value<int>());

                case "Virtual":
                    if (attrInfo.AttributeTypeName == "MultiSelectPicklistType")
                    {
                        var str = (string)token;

                        if (String.IsNullOrEmpty(str))
                            return new OptionSetValueCollection();

                        return new OptionSetValueCollection(str
                            .Split(',')
                            .Select(s => new OptionSetValue(Int32.Parse(s.Trim(), CultureInfo.InvariantCulture)))
                            .ToList());
                    }
                    break;

                case "Money":
                    return new Money(token.Value<decimal>());

                case "DateTime":
                    return WebApiJson.ParseDateTime(token);

                case "Uniqueidentifier":
                    return Guid.Parse((string)token);

                case "Boolean":
                    if (token.Type == JTokenType.Boolean)
                        return (bool)token;
                    if (token.Type == JTokenType.Integer)
                        return token.Value<int>() != 0;
                    return Boolean.Parse((string)token);

                case "Integer":
                    return token.Value<int>();

                case "BigInt":
                    return token.Value<long>();

                case "Decimal":
                    return token.Value<decimal>();

                case "Double":
                    return token.Value<double>();

                case "String":
                case "Memo":
                case "EntityName":
                    return (string)token;
            }

            if (attrInfo?.AttributeTypeName == "ImageType" || attrInfo?.AttributeTypeName == "FileType")
            {
                if (token.Type == JTokenType.String && attrInfo.AttributeTypeName == "ImageType")
                {
                    try
                    {
                        return Convert.FromBase64String((string)token);
                    }
                    catch (FormatException)
                    {
                        return (string)token;
                    }
                }
            }

            return WebApiJson.ToClrValue(token);
        }
    }
}
