using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Xml.Serialization;

namespace MarkMpn.Sql4Cds.Engine
{
    public abstract class Query
    {
        public abstract void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options);

        public object Result { get; protected set; }
    }

    public abstract class FetchXmlQuery : Query
    {
        public FetchXml.FetchType FetchXml { get; set; }

        public bool AllPages { get; set; }

        protected EntityCollection RetrieveAll(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                return null;

            var res = new EntityCollection(RetrieveSequence(org, metadata, options).ToList());
            res.EntityName = FetchXml.Items.OfType<FetchEntityType>().Single().name;

            return res;
        }

        protected IEnumerable<Entity> RetrieveSequence(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                yield break;

            var mainEntity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var name = mainEntity.name;
            var meta = metadata[name];
            options.Progress($"Retrieving {meta.DisplayCollectionName.UserLocalizedLabel.Label}...");

            var res = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

            foreach (var entity in res.Entities)
                yield return entity;

            var count = res.Entities.Count;

            if (AllPages && FetchXml.aggregateSpecified && FetchXml.aggregate && count == 5000 && FetchXml.top != "5000" && !res.MoreRecords)
                throw new ApplicationException("AggregateQueryRecordLimit");

            while (!options.Cancelled && AllPages && res.MoreRecords && options.ContinueRetrieve(count))
            {
                options.Progress($"Retrieved {count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label}...");

                if (FetchXml.page == null)
                    FetchXml.page = "2";
                else
                    FetchXml.page = (Int32.Parse(FetchXml.page) + 1).ToString();

                FetchXml.pagingcookie = res.PagingCookie;

                var nextPage = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

                foreach (var entity in nextPage.Entities)
                    yield return entity;

                count += nextPage.Entities.Count;
                res = nextPage;
            }
        }

        public static string Serialize(FetchXml.FetchType fetch)
        {
            var serializer = new XmlSerializer(typeof(FetchXml.FetchType));

            using (var writer = new StringWriter())
            using (var xmlWriter = System.Xml.XmlWriter.Create(writer, new System.Xml.XmlWriterSettings
            {
                OmitXmlDeclaration = true,
                Indent = true
            }))
            {

                var xsn = new XmlSerializerNamespaces();
                xsn.Add("generator", "MarkMpn.SQL4CDS");

                serializer.Serialize(xmlWriter, fetch, xsn);
                return writer.ToString();
            }
        }
    }

    public class SelectQuery : FetchXmlQuery
    {
        public override void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (RetrieveTotalRecordCount(org, metadata))
                return;

            try
            {
                Result = RetrieveAll(org, metadata, options);
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("AggregateQueryRecordLimit"))
                    throw;

                if (!RetrieveManualAggregate(org, metadata, options))
                    throw new ApplicationException("Unable to apply custom aggregation for large datasets when using DATEPART", ex);
            }
        }

        private bool RetrieveTotalRecordCount(IOrganizationService org, IAttributeMetadataCache metadata)
        {
            // Special case - SELECT count(primaryid) with no filter
            if (!FetchXml.aggregate)
                return false;
            
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var attributes = entity.Items.OfType<FetchAttributeType>().ToArray();

            if (attributes.Length != 1 || attributes[0].aggregate != AggregateType.count)
                return false;

            var filters = entity.Items.OfType<filter>().Count();
            var links = entity.Items.OfType<FetchLinkEntityType>().Count();

            if (filters != 0 || links != 0)
                return false;

            if (attributes[0].name != metadata[entity.name].PrimaryIdAttribute)
                return false;

            // RetrieveTotalRecordCountRequest is only supported in v9+
            var version = (RetrieveVersionResponse)org.Execute(new RetrieveVersionRequest());

            if (!Version.TryParse(version.Version, out var serverVersion) || serverVersion.Major < 9)
                return false;

            var count = ((RetrieveTotalRecordCountResponse)org.Execute(new RetrieveTotalRecordCountRequest { EntityNames = new[] { entity.name } })).EntityRecordCountCollection[entity.name];

            var resultEntity = new Entity(entity.name)
            {
                [attributes[0].alias] = new AliasedValue(entity.name, attributes[0].name, count)
            };

            Result = new EntityCollection { EntityName = entity.name, Entities = { resultEntity } };
            return true;
        }

        private bool RetrieveManualAggregate(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // Remove aggregate flags
            FetchXml.aggregate = false;
            FetchXml.aggregateSpecified = false;

            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var aggregates = new Dictionary<string, Aggregate>();
            RemoveAggregate(entity.Items, aggregates);

            // Remove groupby flags
            var groupByAttributes = new List<FetchAttributeType>();
            RemoveGroupBy(entity.Items, groupByAttributes);

            // Can't handle manual grouping by date parts without much more work
            if (groupByAttributes.Any(a => a.dategroupingSpecified))
                return false;

            // Ensure sort order follows groupby attributes
            var sorts = SortByGroups(entity, groupByAttributes);

            var top = String.IsNullOrEmpty(FetchXml.top) ? (int?)null : Int32.Parse(FetchXml.top);
            var count = String.IsNullOrEmpty(FetchXml.count) ? (int?)null : Int32.Parse(FetchXml.count);
            var page = String.IsNullOrEmpty(FetchXml.page) ? (int?)null : Int32.Parse(FetchXml.page);

            FetchXml.top = null;
            FetchXml.count = null;
            FetchXml.page = null;

            // Retrieve records and track aggregates per group
            var result = RetrieveSequence(org, metadata, options)
                .AggregateGroupBy(groupByAttributes.Select(a => a.alias).ToList(), aggregates, options);

            // Manually re-apply original orders
            if (sorts != null)
                result = result.OrderBy(sorts);
            
            // Apply top/page
            if (top != null)
                result = result.Take(top.Value);
            else if (page != null && count != null)
                result = result.Skip((page.Value - 1) * count.Value).Take(count.Value);

            Result = new EntityCollection(result.ToList())
            {
                EntityName = entity.name
            };

            return true;
        }

        private void RemoveAggregate(object[] items, IDictionary<string,Aggregate> aggregates)
        {
            foreach (var attr in items.OfType<FetchAttributeType>().Where(a => a.aggregateSpecified))
            {
                Aggregate aggregate = null;

                switch (attr.aggregate)
                {
                    case AggregateType.avg:
                        aggregate = new Average();
                        break;

                    case AggregateType.count:
                        aggregate = new Count();
                        break;

                    case AggregateType.countcolumn:
                        aggregate = attr.distinctSpecified && attr.distinct == FetchBoolType.@true ? (Aggregate) new CountColumnDistinct() : new CountColumn();
                        break;

                    case AggregateType.max:
                        aggregate = new Max();
                        break;

                    case AggregateType.min:
                        aggregate = new Min();
                        break;

                    case AggregateType.sum:
                        aggregate = new Sum();
                        break;
                }

                aggregates[attr.alias] = aggregate;

                attr.aggregateSpecified = false;
                attr.distinctSpecified = false;
            }

            foreach (var link in items.OfType<FetchLinkEntityType>())
                RemoveAggregate(link.Items, aggregates);
        }

        private void RemoveGroupBy(object[] items, List<FetchAttributeType> groupByAttributes)
        {
            foreach (var attr in items.OfType<FetchAttributeType>().Where(a => a.groupbySpecified && a.groupby == FetchBoolType.@true))
            {
                groupByAttributes.Add(new FetchAttributeType
                {
                    groupby = FetchBoolType.@true,
                    groupbySpecified = true,
                    name = attr.name,
                    alias = attr.alias,
                    dategroupingSpecified = attr.dategroupingSpecified
                });

                attr.groupby = FetchBoolType.@false;
                attr.groupbySpecified = false;
            }

            foreach (var link in items.OfType<FetchLinkEntityType>())
                RemoveGroupBy(link.Items, groupByAttributes);
        }

        private FetchOrderType[] SortByGroups(FetchEntityType entity, List<FetchAttributeType> groupByAttributes)
        {
            var unsortedGroupByAttributes = new HashSet<string>(groupByAttributes.Select(attr => attr.alias));
            var requiredSorts = new List<FetchOrderType>();

            entity.Items = SortByGroups(entity.Items, groupByAttributes, unsortedGroupByAttributes, requiredSorts);

            return requiredSorts.ToArray();
        }

        private object[] SortByGroups(object[] items, List<FetchAttributeType> groupByAttributes, HashSet<string> unsortedGroupByAttributes, List<FetchOrderType> requiredSorts)
        {
            if (items == null)
                return null;

            var sorts = items.OfType<FetchOrderType>().ToArray();
            
            for (var i = 0; i < sorts.Length; i++)
            {
                var attr = groupByAttributes.SingleOrDefault(a => a.alias.Equals(sorts[i].alias, StringComparison.OrdinalIgnoreCase));

                if (attr != null && unsortedGroupByAttributes.Remove(attr.alias))
                {
                    // Sort by attributes instead of aliases
                    sorts[i].alias = null;
                    sorts[i].attribute = attr.name;

                    // Ensure the attribute is included without an alias
                    if (!String.IsNullOrEmpty(attr.alias) && attr.alias != attr.name)
                        items = items.Concat(new object[] { new FetchAttributeType { name = attr.name } }).ToArray();

                    continue;
                }

                // Remove this unnecessary sort
                items = items.Except(new[] { sorts[i] }).ToArray();

                // Indicate that we need to re-sort the results later
                requiredSorts.Add(sorts[i]);
            }

            items = items.Concat(unsortedGroupByAttributes
                    .Where(a => items.OfType<FetchAttributeType>().Any(attr => attr.alias == a))
                    .Select(a => new FetchOrderType { attribute = a })
                ).ToArray();

            var links = items.OfType<FetchLinkEntityType>();

            foreach (var link in links)
                link.Items = SortByGroups(link.Items, groupByAttributes, unsortedGroupByAttributes, requiredSorts);

            return items;
        }

        public string[] ColumnSet { get; set; }
    }

    public class UpdateQuery : FetchXmlQuery
    {
        public string EntityName { get; set; }

        public string IdColumn { get; set; }

        public IDictionary<string,object> Updates { get; set; }

        public override void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                return;

            if (options.BlockUpdateWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any())
                throw new InvalidOperationException("UPDATE without WHERE is blocked by your settings");

            var count = 0;
            var entities = RetrieveAll(org, metadata, options).Entities;

            if (entities == null)
                return;

            var meta = metadata[EntityName];

            if (!options.ConfirmUpdate(entities.Count, meta))
                throw new OperationCanceledException("UPDATE cancelled by user");

            ExecuteMultipleRequest multiple = null;

            foreach (var entity in entities)
            {
                if (options.Cancelled)
                    break;

                var id = entity[IdColumn];
                if (id is AliasedValue alias)
                    id = alias.Value;

                var update = new Entity(EntityName);
                update.Id = (Guid)id;

                foreach (var attr in Updates)
                    update[attr.Key] = attr.Value;

                if (options.BatchSize == 1)
                {
                    options.Progress($"Updating {meta.DisplayName.UserLocalizedLabel.Label} {count + 1:N0} of {entities.Count:N0}...");
                    org.Update(update);
                    count++;
                }
                else
                {
                    if (multiple == null)
                    {
                        multiple = new ExecuteMultipleRequest
                        {
                            Requests = new OrganizationRequestCollection(),
                            Settings = new ExecuteMultipleSettings
                            {
                                ContinueOnError = false,
                                ReturnResponses = false
                            }
                        };
                    }

                    multiple.Requests.Add(new UpdateRequest { Target = update });

                    if (multiple.Requests.Count == options.BatchSize)
                    {
                        options.Progress($"Updating {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                        var resp = (ExecuteMultipleResponse) org.Execute(multiple);
                        if (resp.IsFaulted)
                            throw new ApplicationException($"Error updating {meta.DisplayCollectionName.UserLocalizedLabel.Label}");

                        count += multiple.Requests.Count;

                        multiple = null;
                    }
                }
            }

            if (!options.Cancelled && multiple != null)
            {
                options.Progress($"Updating {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                var resp = (ExecuteMultipleResponse)org.Execute(multiple);
                if (resp.IsFaulted)
                    throw new ApplicationException($"Error updating {meta.DisplayCollectionName.UserLocalizedLabel.Label}");

                count += multiple.Requests.Count;
            }

            Result = $"{count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} updated";
        }
    }

    public class DeleteQuery : FetchXmlQuery
    {
        public string EntityName { get; set; }

        public string IdColumn { get; set; }

        public override void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                return;

            if (options.BlockDeleteWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any())
                throw new InvalidOperationException("DELETE without WHERE is blocked by your settings");

            var meta = metadata[EntityName];

            if (options.UseBulkDelete)
            {
                var query = ((FetchXmlToQueryExpressionResponse)org.Execute(new FetchXmlToQueryExpressionRequest { FetchXml = Serialize(FetchXml) })).Query;

                var bulkDelete = new BulkDeleteRequest
                {
                    JobName = $"SQL 4 CDS {meta.DisplayCollectionName.UserLocalizedLabel.Label} Bulk Delete Job",
                    QuerySet = new[] { query },
                    StartDateTime = DateTime.Now,
                    RunNow = true,
                    RecurrencePattern = String.Empty,
                    SendEmailNotification = false,
                    ToRecipients = new Guid[0],
                    CCRecipients = new Guid[0]
                };

                org.Execute(bulkDelete);

                Result = "Bulk delete job started";
                return;
            }

            var count = 0;
            var entities = RetrieveAll(org, metadata, options).Entities;

            if (entities == null)
                return;

            if (!options.ConfirmDelete(entities.Count, meta))
                throw new OperationCanceledException("DELETE cancelled by user");

            ExecuteMultipleRequest multiple = null;

            foreach (var entity in entities)
            {
                if (options.Cancelled)
                    break;

                var id = entity[IdColumn];
                if (id is AliasedValue alias)
                    id = alias.Value;

                if (options.BatchSize == 1)
                {
                    options.Progress($"Deleting {meta.DisplayName.UserLocalizedLabel.Label} {count + 1:N0} of {entities.Count:N0}...");
                    org.Delete(EntityName, (Guid)id);
                    count++;
                }
                else
                {
                    if (multiple == null)
                    {
                        multiple = new ExecuteMultipleRequest
                        {
                            Requests = new OrganizationRequestCollection(),
                            Settings = new ExecuteMultipleSettings
                            {
                                ContinueOnError = false,
                                ReturnResponses = false
                            }
                        };
                    }

                    multiple.Requests.Add(new DeleteRequest { Target = new EntityReference(EntityName, (Guid)id) });

                    if (multiple.Requests.Count == options.BatchSize)
                    {
                        options.Progress($"Deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                        var resp = (ExecuteMultipleResponse)org.Execute(multiple);
                        if (resp.IsFaulted)
                            throw new ApplicationException($"Error deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label}");

                        count += multiple.Requests.Count;

                        multiple = null;
                    }
                }
            }

            if (!options.Cancelled && multiple != null)
            {
                options.Progress($"Deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0}...");
                var resp = (ExecuteMultipleResponse)org.Execute(multiple);
                if (resp.IsFaulted)
                    throw new ApplicationException($"Error deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label}");

                count += multiple.Requests.Count;
            }

            Result = $"{count} {meta.DisplayCollectionName.UserLocalizedLabel.Label} deleted";
        }
    }

    public class InsertValues : Query
    {
        public string LogicalName { get; set; }
        public IDictionary<string, object>[] Values { get; set; }

        public override void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var meta = metadata[LogicalName];

            for (var i = 0; i < Values.Length && !options.Cancelled; i++)
            {
                var entity = new Entity(LogicalName);

                foreach (var attr in Values[i])
                    entity[attr.Key] = attr.Value;

                org.Create(entity);

                options.Progress($"Inserted {i:N0} of {Values.Length:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} ({(float)i / Values.Length:P0})");
            }

            Result = $"{Values.Length} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }

    public class InsertSelect : FetchXmlQuery
    {
        public string LogicalName { get; set; }
        public IDictionary<string, string> Mappings { get; set; }

        public override void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var count = 0;
            var entities = RetrieveAll(org, metadata, options).Entities;

            if (entities == null)
                return;

            var meta = metadata[LogicalName];

            foreach (var entity in entities)
            {
                if (options.Cancelled)
                    break;

                var newEntity = new Entity(LogicalName);

                foreach (var attr in Mappings)
                {
                    object value = null;

                    if (entity.Contains(attr.Key))
                        value = entity[attr.Key];

                    if (value is Guid g)
                        value = new EntityReference(entity.LogicalName, g);

                    newEntity[attr.Value] = value;
                }

                org.Create(newEntity);

                count++;
                options.Progress($"Inserted {count:N0} of {entities.Count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} ({(float)count / entities.Count:P0})");
            }

            Result = $"{count} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }
}
