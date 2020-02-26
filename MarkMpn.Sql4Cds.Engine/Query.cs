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
    /// <summary>
    /// A CDS query that can be executed
    /// </summary>
    /// <remarks>
    /// Further details for specific query types can be found from one of the derived types, but all query types can be executed
    /// using the same <see cref="Execute(IOrganizationService, IAttributeMetadataCache, IQueryExecutionOptions)"/> method.
    /// </remarks>
    public abstract class Query
    {
        /// <summary>
        /// Executes the query
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <remarks>
        /// After calling this method, the results can be retrieved from the <see cref="Result"/> property.
        /// </remarks>
        public void Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            try
            {
                Result = ExecuteInternal(org, metadata, options);
            }
            catch (Exception ex)
            {
                Result = ex;
            }
        }

        /// <summary>
        /// Performs the actual query execution. Any exception thrown here will be captured in the <see cref="Result"/> property.
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        protected abstract object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options);

        /// <summary>
        /// The results of the query after <see cref="Execute(IOrganizationService, IAttributeMetadataCache, IQueryExecutionOptions)"/>
        /// has been called.
        /// </summary>
        /// <remarks>
        /// Depending on the type of query, this property can return a <see cref="EntityCollection"/> (for a <see cref="SelectQuery"/>),
        /// an exception if the query failed, or a <see cref="String"/> value containing a human-readable description of the query status.
        /// </remarks>
        public object Result { get; private set; }
    }

    /// <summary>
    /// A CDS query that is based on FetchXML
    /// </summary>
    public abstract class FetchXmlQuery : Query
    {
        /// <summary>
        /// The FetchXML query
        /// </summary>
        public FetchType FetchXml { get; set; }

        /// <summary>
        /// Indicates if the query will page across all the available data
        /// </summary>
        public bool AllPages { get; set; }

        /// <summary>
        /// The additional predicate to be applied to the results
        /// </summary>
        public Func<Entity,bool> PostFilter { get; set; }

        /// <summary>
        /// The details of the calculated fields to generate
        /// </summary>
        public IDictionary<string,Func<Entity,object>> CalculatedFields { get; set; }

        /// <summary>
        /// The additional sorts to by applied to the results
        /// </summary>
        public SortExpression[] PostSorts { get; set; }

        /// <summary>
        /// The final number of records to retrieve
        /// </summary>
        public int? PostTop { get; set; }

        /// <summary>
        /// Retrieves all the data matched by the <see cref="FetchXml"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <returns>The records matched by the query</returns>
        protected EntityCollection RetrieveAll(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                return null;

            var res = new EntityCollection(RetrieveSequence(org, metadata, options).ToList());
            res.EntityName = FetchXml.Items.OfType<FetchEntityType>().Single().name;

            return res;
        }

        /// <summary>
        /// Retrieves all the data matched by the <see cref="FetchXml"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <returns>The records matched by the query, with any custom filters, calculated fields and sorted applied</returns>
        protected IEnumerable<Entity> RetrieveSequence(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var sequence = RetrieveSequenceInternal(org, metadata, options);

            if (PostSorts != null)
                sequence = sequence.OrderBy(PostSorts);

            if (PostTop != null)
                sequence = sequence.Take(PostTop.Value);

            return sequence;
        }

        /// <summary>
        /// Retrieves all the data matched by the <see cref="FetchXml"/>
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <returns>The records matched by the query, with any custom filters and calculated fields applied</returns>
        private IEnumerable<Entity> RetrieveSequenceInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                yield break;

            var mainEntity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var name = mainEntity.name;
            var meta = metadata[name];
            options.Progress($"Retrieving {meta.DisplayCollectionName?.UserLocalizedLabel?.Label}...");

            // Get the first page of results
            var res = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

            foreach (var entity in res.Entities)
            {
                if (PostFilter == null || PostFilter(entity))
                {
                    AddCalculatedFields(entity);
                    yield return entity;
                }
            }

            var count = res.Entities.Count;

            // Aggregate queries return up to 5000 records and don't provide a method to move on to the next page
            // Throw an exception to indicate the error to the caller
            if (AllPages && FetchXml.aggregateSpecified && FetchXml.aggregate && count == 5000 && FetchXml.top != "5000" && !res.MoreRecords)
                throw new ApplicationException("AggregateQueryRecordLimit");

            // Move on to subsequent pages
            while (AllPages && res.MoreRecords && !options.Cancelled && options.ContinueRetrieve(count))
            {
                options.Progress($"Retrieved {count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label}...");

                if (FetchXml.page == null)
                    FetchXml.page = "2";
                else
                    FetchXml.page = (Int32.Parse(FetchXml.page) + 1).ToString();

                FetchXml.pagingcookie = res.PagingCookie;

                var nextPage = org.RetrieveMultiple(new FetchExpression(Serialize(FetchXml)));

                foreach (var entity in nextPage.Entities)
                {
                    if (PostFilter == null || PostFilter(entity))
                    {
                        AddCalculatedFields(entity);
                        yield return entity;
                    }
                }

                count += nextPage.Entities.Count;
                res = nextPage;
            }
        }

        private void AddCalculatedFields(Entity entity)
        {
            if (CalculatedFields == null)
                return;

            foreach (var calculation in CalculatedFields)
                entity[calculation.Key] = calculation.Value(entity);
        }

        /// <summary>
        /// Convert the FetchXML query object to a string
        /// </summary>
        /// <param name="fetch">The FetchXML query object to convert</param>
        /// <returns>The string representation of the query</returns>
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
                // Add in a separate namespace to remove the xsi and xsd namespaces added by default
                var xsn = new XmlSerializerNamespaces();
                xsn.Add("generator", "MarkMpn.SQL4CDS");

                serializer.Serialize(xmlWriter, fetch, xsn);
                return writer.ToString();
            }
        }
    }

    /// <summary>
    /// A SELECT query to return data using FetchXML
    /// </summary>
    public class SelectQuery : FetchXmlQuery
    {
        /// <inheritdoc/>
        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // Shortcut getting the total number of records in an entity where possible
            if (RetrieveTotalRecordCount(org, metadata, out var result))
                return result;

            try
            {
                // Start off trying to execute the original FetchXML
                return RetrieveAll(org, metadata, options);
            }
            catch (Exception ex)
            {
                // Attempt to handle aggregate queries that go over the standard FetchXML limit by rewriting them to retrieve the
                // individual records and apply the aggregation in-memory
                if (!ex.Message.Contains("AggregateQueryRecordLimit"))
                    throw;

                if (!RetrieveManualAggregate(org, metadata, options, out result))
                    throw new ApplicationException("Unable to apply custom aggregation for large datasets when using DATEPART", ex);

                return result;
            }
        }

        /// <summary>
        /// Get the total number of records in an entity
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <returns><c>true</c> if this method has retrieved the requested details, or <c>false</c> otherwise</returns>
        private bool RetrieveTotalRecordCount(IOrganizationService org, IAttributeMetadataCache metadata, out EntityCollection result)
        {
            result = null;

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

            result = new EntityCollection { EntityName = entity.name, Entities = { resultEntity } };
            return true;
        }

        /// <summary>
        /// Rewrites an aggregate query to retrieve the individual records and calculate the aggregates in-memory
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <returns><c>true</c> if this method has retrieved the requested details, or <c>false</c> otherwise</returns>
        private bool RetrieveManualAggregate(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, out EntityCollection results)
        {
            results = null;

            // Remove aggregate flags
            FetchXml.aggregate = false;
            FetchXml.aggregateSpecified = false;

            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var aggregates = new Dictionary<string, Aggregate>();
            RemoveAggregate(entity.Items, aggregates);

            // Remove groupby flags
            var groupByAttributes = new List<FetchAttributeType>();
            RemoveGroupBy(entity.Items, groupByAttributes);

            // Can't handle manual grouping by date parts as we can't get CDS to sort the data appropriately
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

            results = new EntityCollection(result.ToList())
            {
                EntityName = entity.name
            };

            return true;
        }

        /// <summary>
        /// Find any aggregates specified in the FetchXML, unset the <see cref="FetchAttributeType.aggregate"/> attribute and
        /// add the corresponding calculation to the <paramref name="aggregates"/> list to be calculated in-memory
        /// </summary>
        /// <param name="items">The FetchXML items in an &lt;entity&gt; or &lt;link-entity&gt;</param>
        /// <param name="aggregates">A mapping from output attribute alias to the aggregate calculation to use to generate it</param>
        private void RemoveAggregate(object[] items, IDictionary<string,Aggregate> aggregates)
        {
            if (items == null)
                return;

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

        /// <summary>
        /// Find any groupings applied in the FetchXML, unset the <see cref="FetchAttributeType.groupby"/> attribute and add the details
        /// of the grouping to apply to the <paramref name="groupByAttributes"/> list
        /// </summary>
        /// <param name="items">The FetchXML items in an &lt;entity&gt; or &lt;link-entity&gt;</param>
        /// <param name="groupByAttributes">The grouping attributes to apply to the output</param>
        private void RemoveGroupBy(object[] items, List<FetchAttributeType> groupByAttributes)
        {
            if (items == null)
                return;

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

        /// <summary>
        /// Ensures the results retrieved from CDS will be sorted by the attributes we want to group by, so all records
        /// in a group are retrieved sequentially
        /// </summary>
        /// <param name="entity">The root &lt;entity&gt; in the query</param>
        /// <param name="groupByAttributes">The attributes that the query should group by</param>
        /// <returns></returns>
        private FetchOrderType[] SortByGroups(FetchEntityType entity, List<FetchAttributeType> groupByAttributes)
        {
            // Keep track of which grouping attributes haven't currently got a sort order applied and which
            // sorts will need to be applied at the end of the query
            var unsortedGroupByAttributes = new HashSet<string>(groupByAttributes.Select(attr => attr.alias));
            var requiredSorts = new List<FetchOrderType>();

            entity.Items = SortByGroups(entity.Items, groupByAttributes, unsortedGroupByAttributes, requiredSorts);

            return requiredSorts.ToArray();
        }

        /// <summary>
        /// Checks the groupings applied to an &lt;entity&gt; or &lt;link-entity&gt; to ensure the results are correctly sorted
        /// </summary>
        /// <param name="items">The FetchXML items in the &lt;entity&gt; or &lt;link-entity&gt;</param>
        /// <param name="groupByAttributes">The attributes that the query is grouped by</param>
        /// <param name="unsortedGroupByAttributes">The grouping attributes that haven't had a sort identified for them yet</param>
        /// <param name="requiredSorts">The sorts that need to be applied to the final results</param>
        /// <returns>The FetchXML items to use in place of the supplied <paramref name="items"/></returns>
        private object[] SortByGroups(object[] items, List<FetchAttributeType> groupByAttributes, HashSet<string> unsortedGroupByAttributes, List<FetchOrderType> requiredSorts)
        {
            if (items == null)
                return null;

            // Check through each of the sorts in the current items to see which are needed and which can be removed
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

                    // Re-apply the sort at the end as well to ensure the ordering remains consistent
                    requiredSorts.Add(sorts[i]);

                    continue;
                }

                // Remove this unnecessary sort
                items = items.Except(new[] { sorts[i] }).ToArray();

                // Indicate that we need to re-sort the results later
                requiredSorts.Add(sorts[i]);
            }

            // Add any more sorts required for grouping attributes in this entity that don't already have a sort
            items = items.Concat(unsortedGroupByAttributes
                    .Where(a => items.OfType<FetchAttributeType>().Any(attr => attr.alias == a))
                    .Select(a => new FetchOrderType { attribute = a })
                ).ToArray();

            // Recurse through any link-entities
            var links = items.OfType<FetchLinkEntityType>();

            foreach (var link in links)
                link.Items = SortByGroups(link.Items, groupByAttributes, unsortedGroupByAttributes, requiredSorts);

            return items;
        }

        /// <summary>
        /// The columns that should be included in the query results
        /// </summary>
        public string[] ColumnSet { get; set; }
    }

    /// <summary>
    /// An UPDATE query to identify records via FetchXML and change them
    /// </summary>
    public class UpdateQuery : FetchXmlQuery
    {
        /// <summary>
        /// The logical name of the entity to update
        /// </summary>
        public string EntityName { get; set; }

        /// <summary>
        /// The primary key attribute in the entity to update
        /// </summary>
        public string IdColumn { get; set; }

        /// <summary>
        /// A mapping of attribute names to values to apply updates to
        /// </summary>
        public IDictionary<string,Func<Entity,object>> Updates { get; set; }

        /// <inheritdoc/>
        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            if (options.Cancelled)
                return null;

            // Check if the update is allowed
            if (options.BlockUpdateWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any())
                throw new InvalidOperationException("UPDATE without WHERE is blocked by your settings");

            // Get the records to update
            var count = 0;
            var entities = RetrieveAll(org, metadata, options).Entities;

            if (entities == null)
                return null;

            var meta = metadata[EntityName];

            // Check again that the update is allowed
            if (!options.ConfirmUpdate(entities.Count, meta))
                throw new OperationCanceledException("UPDATE cancelled by user");

            // Apply the update in batches
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
                    update[attr.Key] = attr.Value(entity);

                if (options.BatchSize == 1)
                {
                    options.Progress($"Updating {meta.DisplayName?.UserLocalizedLabel?.Label} {count + 1:N0} of {entities.Count:N0}...");
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
                        options.Progress($"Updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                        var resp = (ExecuteMultipleResponse) org.Execute(multiple);
                        if (resp.IsFaulted)
                            throw new ApplicationException($"Error updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label}");

                        count += multiple.Requests.Count;

                        multiple = null;
                    }
                }
            }

            if (!options.Cancelled && multiple != null)
            {
                options.Progress($"Updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                var resp = (ExecuteMultipleResponse)org.Execute(multiple);
                if (resp.IsFaulted)
                    throw new ApplicationException($"Error updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label}");

                count += multiple.Requests.Count;
            }

            return $"{count:N0} {meta.DisplayCollectionName?.UserLocalizedLabel?.Label} updated";
        }
    }

    /// <summary>
    /// A DELETE query to identify records via FetchXML and delete them
    /// </summary>
    public class DeleteQuery : FetchXmlQuery
    {
        /// <summary>
        /// The logical name of the entity to delete
        /// </summary>
        public string EntityName { get; set; }

        /// <summary>
        /// The primary key column of the entity to delete
        /// </summary>
        public string IdColumn { get; set; }

        /// <inheritdoc/>
        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // Check if the query is allowed
            if (options.Cancelled)
                return null;

            if (options.BlockDeleteWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any())
                throw new InvalidOperationException("DELETE without WHERE is blocked by your settings");

            var meta = metadata[EntityName];

            // If we are using a bulk delete job, start the job
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

                return "Bulk delete job started";
            }

            // Otherwise, get the records to delete
            var count = 0;
            var entities = RetrieveAll(org, metadata, options).Entities;

            if (entities == null)
                return null;

            // Check again if the query is allowed
            if (!options.ConfirmDelete(entities.Count, meta))
                throw new OperationCanceledException("DELETE cancelled by user");

            ExecuteMultipleRequest multiple = null;

            // Delete hte records in batches
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

            return $"{count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} deleted";
        }
    }

    /// <summary>
    /// An INSERT query to add fixed values
    /// </summary>
    public class InsertValues : Query
    {
        /// <summary>
        /// The logical name of the entity to add
        /// </summary>
        public string LogicalName { get; set; }

        /// <summary>
        /// A list of records to insert
        /// </summary>
        public IDictionary<string, object>[] Values { get; set; }

        /// <inheritdoc/>
        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var meta = metadata[LogicalName];

            // Add each record in turn
            for (var i = 0; i < Values.Length && !options.Cancelled; i++)
            {
                var entity = new Entity(LogicalName);

                foreach (var attr in Values[i])
                    entity[attr.Key] = attr.Value;

                org.Create(entity);

                options.Progress($"Inserted {i:N0} of {Values.Length:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} ({(float)i / Values.Length:P0})");
            }

            return $"{Values.Length:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }

    /// <summary>
    /// An INSERT query to add records based on the results of a FetchXML query
    /// </summary>
    public class InsertSelect : FetchXmlQuery
    {
        /// <summary>
        /// The logical name of the entity to insert
        /// </summary>
        public string LogicalName { get; set; }

        /// <summary>
        /// The mappings of columns from the FetchXML results to the attributes of the entity to insert
        /// </summary>
        public IDictionary<string, string> Mappings { get; set; }

        /// <inheritdoc/>
        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // Get all the records to insert
            var count = 0;
            var entities = RetrieveAll(org, metadata, options).Entities;

            if (entities == null)
                return null;

            var meta = metadata[LogicalName];

            // Insert each record in turn
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

            return $"{count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }
}
