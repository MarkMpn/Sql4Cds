using MarkMpn.Sql4Cds.FetchXml;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Windows.Forms;
using System.Xml.Serialization;

namespace MarkMpn.Sql4Cds
{
    abstract class Query
    {
        public abstract void Execute(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress);

        public object Result { get; protected set; }
    }

    abstract class FetchXmlQuery : Query
    {
        public FetchXml.FetchType FetchXml { get; set; }

        public bool AllPages { get; set; }

        protected EntityCollection RetrieveAll(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            if (cancelled())
                return null;

            var res = new EntityCollection(RetrieveSequence(org, metadata, cancelled, progress).ToList());
            res.EntityName = FetchXml.Items.OfType<FetchEntityType>().Single().name;

            return res;
        }

        protected IEnumerable<Entity> RetrieveSequence(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            if (cancelled())
                yield break;

            var name = FetchXml.Items.OfType<FetchEntityType>().Single().name;
            var meta = metadata[name];
            progress($"Retrieving {meta.DisplayCollectionName.UserLocalizedLabel.Label}...");

            var res = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

            foreach (var entity in res.Entities)
                yield return entity;

            var count = res.Entities.Count;

            while (!cancelled() && AllPages && res.MoreRecords && (Settings.Instance.SelectLimit == 0 || count < Settings.Instance.SelectLimit))
            {
                progress($"Retrieved {count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label}...");

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

    class SelectQuery : FetchXmlQuery
    {
        public override void Execute(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            if (RetrieveTotalRecordCount(org, metadata))
                return;

            try
            {
                Result = RetrieveAll(org, metadata, cancelled, progress);
            }
            catch (Exception ex)
            {
                if (!ex.Message.Contains("AggregateQueryRecordLimit"))
                    throw;

                RetrieveManualAggregate(org, metadata, cancelled, progress);
            }
        }

        private bool RetrieveTotalRecordCount(IOrganizationService org, AttributeMetadataCache metadata)
        {
            // Special case - SELECT count(primaryid) with no filter
            if (!FetchXml.aggregate)
                return false;
            
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var attributes = entity.Items.OfType<FetchAttributeType>().ToArray();

            if (attributes.Length != 1 || attributes[0].aggregate != AggregateType.countcolumn)
                return false;

            var filters = entity.Items.OfType<filter>().Count();
            var links = entity.Items.OfType<FetchLinkEntityType>().Count();

            if (filters != 0 || links != 0)
                return false;

            if (attributes[0].name != metadata[entity.name].PrimaryIdAttribute)
                return false;

            var count = ((RetrieveTotalRecordCountResponse)org.Execute(new RetrieveTotalRecordCountRequest { EntityNames = new[] { entity.name } })).EntityRecordCountCollection[entity.name];

            var resultEntity = new Entity(entity.name)
            {
                [attributes[0].alias] = new AliasedValue(entity.name, attributes[0].name, count)
            };

            Result = new EntityCollection { EntityName = entity.name, Entities = { resultEntity } };
            return true;
        }

        private void RetrieveManualAggregate(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            // Remove aggregate flags
            FetchXml.aggregate = false;
            FetchXml.aggregateSpecified = false;

            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var aggregates = new Dictionary<string, Aggregate>();
            RemoveAggregate(entity.Items, aggregates);

            // Remove groupby flags
            var groupByAttributes = new List<string>();
            RemoveGroupBy(entity.Items, groupByAttributes);

            // Ensure sort order follows groupby attributes
            var sorts = entity.Items.OfType<FetchOrderType>().ToArray();
            var unsortedGroupByAttributes = new List<string>(groupByAttributes);
            var sortRequired = false;

            for (var i = 0; i < sorts.Length && unsortedGroupByAttributes.Count > 0; i++)
            {
                if (unsortedGroupByAttributes.Remove(sorts[i].alias))
                    continue;

                // Remove this unnecessary sort
                entity.Items = entity.Items.Except(new[] { sorts[i] }).ToArray();

                // Indicate that we need to re-sort the results later
                sortRequired = true;
            }

            // Sort by attributes instead of aliases
            foreach (var sort in sorts)
            {
                sort.attribute = sort.alias;
                sort.alias = null;
            }

            entity.Items = entity.Items.Concat(unsortedGroupByAttributes.Select(a => new FetchOrderType { attribute = a })).ToArray();

            var top = String.IsNullOrEmpty(FetchXml.top) ? (int?)null : Int32.Parse(FetchXml.top);
            var count = String.IsNullOrEmpty(FetchXml.count) ? (int?)null : Int32.Parse(FetchXml.count);
            var page = String.IsNullOrEmpty(FetchXml.page) ? (int?)null : Int32.Parse(FetchXml.page);

            FetchXml.top = null;
            FetchXml.count = null;
            FetchXml.page = null;

            // Retrieve records and track aggregates per group
            var result = RetrieveSequence(org, metadata, cancelled, progress)
                .AggregateGroupBy(groupByAttributes, aggregates, cancelled);

            // Manually re-apply original orders
            if (sortRequired)
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

        private void RemoveGroupBy(object[] items, List<string> groupByAttributes)
        {
            foreach (var attr in items.OfType<FetchAttributeType>().Where(a => a.groupbySpecified && a.groupby == FetchBoolType.@true))
            {
                groupByAttributes.Add(attr.alias);
                attr.groupby = FetchBoolType.@false;
                attr.groupbySpecified = false;
            }

            foreach (var link in items.OfType<FetchLinkEntityType>())
                RemoveGroupBy(link.Items, groupByAttributes);
        }

        public string[] ColumnSet { get; set; }
    }

    class UpdateQuery : FetchXmlQuery
    {
        public string EntityName { get; set; }

        public string IdColumn { get; set; }

        public IDictionary<string,object> Updates { get; set; }

        public override void Execute(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            if (cancelled())
                return;

            if (Settings.Instance.BlockUpdateWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any())
                throw new InvalidOperationException("UPDATE without WHERE is blocked by your settings");

            var count = 0;
            var entities = RetrieveAll(org, metadata, cancelled, progress).Entities;

            if (entities == null)
                return;

            var meta = metadata[EntityName];

            if (entities.Count > Settings.Instance.UpdateWarnThreshold)
            {
                var result = MessageBox.Show($"Update will affect {entities.Count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label}. Do you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    throw new OperationCanceledException("UPDATE cancelled by user");
            }

            ExecuteMultipleRequest multiple = null;

            foreach (var entity in entities)
            {
                if (cancelled())
                    break;

                var id = entity[IdColumn];
                if (id is AliasedValue alias)
                    id = alias.Value;

                var update = new Entity(EntityName);
                update.Id = (Guid)id;

                foreach (var attr in Updates)
                    update[attr.Key] = attr.Value;

                if (multiple == null)
                {
                    multiple = new ExecuteMultipleRequest()
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

                if (multiple.Requests.Count == 1000)
                {
                    progress($"Updating {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0}...");
                    org.Execute(multiple);
                    count += multiple.Requests.Count;

                    multiple = null;
                }
            }

            if (!cancelled() && multiple != null)
            {
                progress($"Updating {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0}...");
                org.Execute(multiple);
                count += multiple.Requests.Count;
            }

            Result = $"{count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} updated";
        }
    }

    class DeleteQuery : FetchXmlQuery
    {
        public string EntityName { get; set; }

        public string IdColumn { get; set; }

        public override void Execute(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            if (cancelled())
                return;

            if (Settings.Instance.BlockDeleteWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any())
                throw new InvalidOperationException("DELETE without WHERE is blocked by your settings");

            var meta = metadata[EntityName];

            if (Settings.Instance.UseBulkDelete)
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
            var entities = RetrieveAll(org, metadata, cancelled, progress).Entities;

            if (entities == null)
                return;

            if (entities.Count > Settings.Instance.DeleteWarnThreshold)
            {
                var result = MessageBox.Show($"Delete will affect {entities.Count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label}. Do you want to proceed?", "Confirm", MessageBoxButtons.YesNo, MessageBoxIcon.Warning, MessageBoxDefaultButton.Button2);

                if (result == DialogResult.No)
                    throw new OperationCanceledException("DELETE cancelled by user");
            }

            ExecuteMultipleRequest multiple = null;

            foreach (var entity in entities)
            {
                if (cancelled())
                    break;

                var id = entity[IdColumn];
                if (id is AliasedValue alias)
                    id = alias.Value;

                if (multiple == null)
                {
                    multiple = new ExecuteMultipleRequest()
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

                if (multiple.Requests.Count == 1000)
                {
                    progress($"Deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0}...");
                    org.Execute(multiple);
                    count += multiple.Requests.Count;

                    multiple = null;
                }
            }

            if (!cancelled() && multiple != null)
            {
                progress($"Deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0}...");
                org.Execute(multiple);
                count += multiple.Requests.Count;
            }

            Result = $"{count} {meta.DisplayCollectionName.UserLocalizedLabel.Label} deleted";
        }
    }

    class InsertValues : Query
    {
        public string LogicalName { get; set; }
        public IDictionary<string, object>[] Values { get; set; }

        public override void Execute(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            var meta = metadata[LogicalName];

            for (var i = 0; i < Values.Length && !cancelled(); i++)
            {
                var entity = new Entity(LogicalName);

                foreach (var attr in Values[i])
                    entity[attr.Key] = attr.Value;

                org.Create(entity);

                progress($"Inserted {i:N0} of {Values.Length:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} ({(float)i / Values.Length:P0})");
            }

            Result = $"{Values.Length} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }

    class InsertSelect : FetchXmlQuery
    {
        public string LogicalName { get; set; }
        public IDictionary<string, string> Mappings { get; set; }

        public override void Execute(IOrganizationService org, AttributeMetadataCache metadata, Func<bool> cancelled, Action<string> progress)
        {
            var count = 0;
            var entities = RetrieveAll(org, metadata, cancelled, progress).Entities;

            if (entities == null)
                return;

            var meta = metadata[LogicalName];

            foreach (var entity in entities)
            {
                if (cancelled())
                    break;

                var newEntity = new Entity(LogicalName);

                foreach (var attr in Mappings)
                    newEntity[attr.Value] = entity[attr.Key];

                org.Create(entity);

                count++;
                progress($"Inserted {count:N0} of {entities.Count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} ({(float)count / entities.Count:P0})");
            }

            Result = $"{count} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }
}
