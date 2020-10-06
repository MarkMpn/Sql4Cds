using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;
using Microsoft.Xrm.Tooling.Connector;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Xml;
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
        /// The original SQL that the query was created from
        /// </summary>
        public string Sql { get; set; }

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
        private FetchType _fetch;

        /// <summary>
        /// The FetchXML query
        /// </summary>
        public FetchType FetchXml
        {
            get { return _fetch; }
            set
            {
                _fetch = value;
                FetchXmlString = Serialize(_fetch);
            }
        }

        /// <summary>
        /// The string representation of the <see cref="FetchXml"/>
        /// </summary>
        public string FetchXmlString { get; private set; }

        /// <summary>
        /// Indicates if the query will page across all the available data
        /// </summary>
        public bool AllPages { get; set; }

        /// <summary>
        /// A list of any post-processing functions to be applied to the results
        /// </summary>
        public IList<IQueryExtension> Extensions { get; } = new List<IQueryExtension>();

        /// <summary>
        /// The columns that should be included in the query results
        /// </summary>
        public string[] ColumnSet { get; set; }

        /// <summary>
        /// Returns an alternative query that can be used if the main aggregate query results in an AggregateQueryRecordLimit error
        /// </summary>
        public FetchXmlQuery AggregateAlternative { get; set; }

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

            try
            {
                var res = new EntityCollection(RetrieveSequence(org, metadata, options).ToList());
                res.EntityName = FetchXml.Items.OfType<FetchEntityType>().Single().name;

                return res;
            }
            catch (Exception ex)
            {
                // Attempt to handle aggregate queries that go over the standard FetchXML limit by rewriting them to retrieve the
                // individual records and apply the aggregation in-memory
                if (!ex.Message.Contains("AggregateQueryRecordLimit"))
                    throw;

                if (AggregateAlternative == null)
                    throw;

                AggregateAlternative.Execute(org, metadata, options);
                return (EntityCollection)AggregateAlternative.Result;
            }
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

            foreach (var extension in Extensions)
                sequence = extension.ApplyTo(sequence, options);

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
                yield return entity;

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
                    yield return entity;

                count += nextPage.Entities.Count;
                res = nextPage;
            }
        }

        /// <summary>
        /// Convert the FetchXML query object to a string
        /// </summary>
        /// <param name="fetch">The FetchXML query object to convert</param>
        /// <returns>The string representation of the query</returns>
        internal static string Serialize(FetchType fetch)
        {
            var serializer = new XmlSerializer(typeof(FetchType));

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

            // Run the raw SQL query against the T-SQL endpoint
            if (ExecuteTSQL(org, options, out var dataTable))
                return dataTable;

            // Execute the FetchXML
            return RetrieveAll(org, metadata, options);
        }

        /// <summary>
        /// Run the raw SQL query against the T-SQL endpoint
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="options">The options that indicate if the T-SQL endpoint should be used</param>
        /// <param name="result">The results of running the query</param>
        /// <returns><c>true</c> if this method has executed the query, or <c>false otherwise</c></returns>
        private bool ExecuteTSQL(IOrganizationService org, IQueryExecutionOptions options, out DataTable result)
        {
            result = null;

            if (String.IsNullOrEmpty(Sql))
                return false;

            if (!options.UseTSQLEndpoint)
                return false;

            if (!(org is CrmServiceClient svc))
                return false;

            if (String.IsNullOrEmpty(svc.CurrentAccessToken))
                return false;

            if (!TSqlEndpoint.IsEnabled(svc))
                return false;

            using (var con = new SqlConnection("server=" + svc.CrmConnectOrgUriActual.Host + ",5558"))
            {
                con.AccessToken = svc.CurrentAccessToken;
                con.Open();

                using (var cmd = con.CreateCommand())
                {
                    cmd.CommandText = Sql;
                    result = new DataTable();

                    using (var adapter = new SqlDataAdapter(cmd))
                    {
                        adapter.Fill(result);
                    }

                    return true;
                }
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

            if (FetchXml == null)
                return false;

            if (Extensions.Count > 0)
                return false;

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
            if (options.BlockUpdateWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any() && !Extensions.OfType<Where>().Any())
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
                                ReturnResponses = true
                            }
                        };
                    }

                    multiple.Requests.Add(new UpdateRequest { Target = update });

                    if (multiple.Requests.Count == options.BatchSize)
                    {
                        options.Progress($"Updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                        var resp = (ExecuteMultipleResponse) org.Execute(multiple);
                        if (resp.IsFaulted)
                            throw new ApplicationException($"Error updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label} - " + resp.Responses.First(r => r.Fault != null).Fault.Message);

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
                    throw new ApplicationException($"Error updating {meta.DisplayCollectionName?.UserLocalizedLabel?.Label} - " + resp.Responses.First(r => r.Fault != null).Fault.Message);

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
        /// The primary key columns of the entity to delete
        /// </summary>
        public string[] IdColumns { get; set; }

        /// <inheritdoc/>
        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            // Check if the query is allowed
            if (options.Cancelled)
                return null;

            if (options.BlockDeleteWithoutWhere && !FetchXml.Items.OfType<FetchEntityType>().Single().Items.OfType<filter>().Any() && !Extensions.OfType<Where>().Any())
                throw new InvalidOperationException("DELETE without WHERE is blocked by your settings");

            var meta = metadata[EntityName];

            // If we are using a bulk delete job, start the job
            if (options.UseBulkDelete && Extensions.Count == 0 && meta.IsIntersect != true)
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

                if (options.BatchSize == 1)
                {
                    options.Progress($"Deleting {meta.DisplayName.UserLocalizedLabel.Label} {count + 1:N0} of {entities.Count:N0}...");
                    org.Execute(CreateDeleteRequest(meta, entity));
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
                                ReturnResponses = true
                            }
                        };
                    }

                    multiple.Requests.Add(CreateDeleteRequest(meta, entity));

                    if (multiple.Requests.Count == options.BatchSize)
                    {
                        options.Progress($"Deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} {count + 1:N0} - {count + multiple.Requests.Count:N0} of {entities.Count:N0}...");
                        var resp = (ExecuteMultipleResponse)org.Execute(multiple);
                        if (resp.IsFaulted)
                            throw new ApplicationException($"Error deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} - " + resp.Responses.First(r => r.Fault != null).Fault.Message);

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
                    throw new ApplicationException($"Error deleting {meta.DisplayCollectionName.UserLocalizedLabel.Label} - " + resp.Responses.First(r => r.Fault != null).Fault.Message);

                count += multiple.Requests.Count;
            }

            return $"{count:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} deleted";
        }

        private OrganizationRequest CreateDeleteRequest(EntityMetadata meta, Entity entity)
        {
            // Special case messages for intersect entities
            if (meta.LogicalName == "listmember")
            {
                var listId = entity.GetAliasedAttributeValue<EntityReference>(IdColumns[0]);
                var entityId = entity.GetAliasedAttributeValue<EntityReference>(IdColumns[1]);

                return new RemoveMemberListRequest { ListId = listId.Id, EntityId = entityId.Id };
            }
            else if (meta.IsIntersect == true)
            {
                var entity1Id = entity.GetAliasedAttributeValue<EntityReference>(IdColumns[0]);
                var entity2Id = entity.GetAliasedAttributeValue<EntityReference>(IdColumns[1]);
                var relationship = meta.ManyToManyRelationships.Single();

                return new DisassociateRequest
                {
                    Target = entity1Id,
                    RelatedEntities = new EntityReferenceCollection(new[] { entity2Id }),
                    Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing }
                };
            }
            else
            {
                var id = entity.GetAliasedAttributeValue<Guid>(IdColumns[0]);

                return new DeleteRequest { Target = new EntityReference(EntityName, id) };
            }
        }
    }

    public abstract class InsertQuery : Query
    {
        /// <summary>
        /// The logical name of the entity to add
        /// </summary>
        public string LogicalName { get; set; }

        /// <summary>
        /// Returns a sequence of the entities to insert
        /// </summary>
        /// <returns></returns>
        protected abstract Entity[] GetValues(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options);

        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            var meta = metadata[LogicalName];

            // Add each record in turn
            var count = 0;
            var entities = GetValues(org, metadata, options);

            if (entities != null)
            {
                foreach (var entity in entities)
                {
                    if (options.Cancelled)
                        break;

                    // Special cases for intersect entities
                    if (LogicalName == "listmember")
                    {
                        var listId = entity.GetAttributeValue<EntityReference>("listid");
                        var entityId = entity.GetAttributeValue<EntityReference>("entityid");

                        if (listId == null)
                            throw new ApplicationException("listid is required");

                        if (entityId == null)
                            throw new ApplicationException("entityid is required");

                        org.Execute(new AddMemberListRequest
                        {
                            ListId = listId.Id,
                            EntityId = entityId.Id
                        });
                    }
                    else if (meta.IsIntersect == true)
                    {
                        // For generic intersect entities we expect a single many-to-many relationship in the metadata which describes
                        // the relationship that this is the intersect entity for
                        var relationship = meta.ManyToManyRelationships.Single();

                        var entity1 = entity.GetAttributeValue<EntityReference>(relationship.Entity1IntersectAttribute);
                        var entity2 = entity.GetAttributeValue<EntityReference>(relationship.Entity2IntersectAttribute);

                        if (entity1 == null)
                            throw new ApplicationException($"{relationship.Entity1IntersectAttribute} is required");

                        if (entity2 == null)
                            throw new ApplicationException($"{relationship.Entity2IntersectAttribute} is required");

                        org.Execute(new AssociateRequest
                        {
                            Target = entity1,
                            Relationship = new Relationship(relationship.SchemaName) { PrimaryEntityRole = EntityRole.Referencing },
                            RelatedEntities = new EntityReferenceCollection(new[] { entity2 })
                        });
                    }
                    else
                    {
                        org.Create(entity);
                    }

                    count++;

                    options.Progress($"Inserted {count:N0} of {entities.Length:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} ({(float)count / entities.Length:P0})");
                }
            }

            return $"{entities.Length:N0} {meta.DisplayCollectionName.UserLocalizedLabel.Label} inserted";
        }
    }

    /// <summary>
    /// An INSERT query to add fixed values
    /// </summary>
    public class InsertValues : InsertQuery
    {
        /// <summary>
        /// A list of records to insert
        /// </summary>
        public IDictionary<string, object>[] Values { get; set; }

        protected override Entity[] GetValues(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            return Values
                .Select(dictionary =>
                {
                    var entity = new Entity(LogicalName);

                    foreach (var attr in dictionary)
                        entity[attr.Key] = attr.Value;

                    return entity;
                })
                .ToArray();
        }
    }

    /// <summary>
    /// An INSERT query to add records based on the results of a FetchXML query
    /// </summary>
    public class InsertSelect : InsertQuery
    {
        /// <summary>
        /// The SELECT query that produces the values to insert
        /// </summary>
        public SelectQuery Source { get; set; }

        /// <summary>
        /// The mappings of columns from the FetchXML results to the attributes of the entity to insert
        /// </summary>
        public IDictionary<string, string> Mappings { get; set; }

        protected override Entity[] GetValues(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            Source.Execute(org, metadata, options);

            if (Source.Result is Exception ex)
                throw ex;

            if (!(Source.Result is EntityCollection entities))
                return null;

            var converted = new List<Entity>(entities.Entities.Count);

            foreach (var entity in entities.Entities)
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

                converted.Add(newEntity);
            }

            return converted.ToArray();
        }
    }

    /// <summary>
    /// A SELECT request that retrieves metadata using a specialized request
    /// </summary>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    public abstract class MetadataQuery<TRequest, TResponse> : SelectQuery
        where TRequest : OrganizationRequest
        where TResponse : OrganizationResponse
    {
        /// <summary>
        /// Returns or sets the metadata request to execute
        /// </summary>
        public TRequest Request { get; set; }

        /// <summary>
        /// Extracts the data in the response as a set of entities
        /// </summary>
        /// <param name="response">The response from the metadata request</param>
        /// <returns>The data contained in the response, split y entity type</returns>
        protected abstract IDictionary<string, IDictionary<Guid, Entity>> GetData(TResponse response);

        /// <summary>
        /// Applies any final changes to the <see cref="Request"/> before it is executed
        /// </summary>
        /// <param name="org">The organization service that will execute the request</param>
        public virtual void FinalizeRequest(IOrganizationService org)
        {
        }

        protected override object ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options)
        {
            options.Progress($"Executing {Request.GetType().Name}...");

            FinalizeRequest(org);
            var response = (TResponse) org.Execute(Request);

            // Convert the response to entities and execute the FetchXML request over that data
            var data = GetData(response);
            var sequence = ExecuteFetchRequest(data);

            // Apply any extra query extensions that are required to the results
            foreach (var extension in Extensions)
                sequence = extension.ApplyTo(sequence, options);

            // Convert the results to a data table. Returning the results as an EntityCollection
            // will confuse the CrmGridView as the metadata doesn't really exist
            var entities = sequence.ToList();
            var table = new DataTable();

            foreach (var col in ColumnSet.Distinct())
            {
                var type = typeof(string);
                var entity = entities.FirstOrDefault(e => e.Contains(col) && e[col] != null && (!(e[col] is AliasedValue av) || av.Value != null));

                if (entity != null)
                {
                    var value = entity[col];

                    if (value is AliasedValue a)
                        value = a.Value;

                    if (value is EntityReference er)
                        value = er.Id;

                    type = value.GetType();
                }

                table.Columns.Add(col, type);
            }

            foreach (var entity in entities)
            {
                var row = table.NewRow();

                foreach (var col in ColumnSet)
                {
                    object value = null;

                    if (entity.Contains(col))
                        value = entity[col];

                    if (value is AliasedValue a)
                        value = a.Value;

                    if (value is EntityReference er)
                        value = er.Id;

                    row[col] = value ?? DBNull.Value;
                }

                table.Rows.Add(row);
            }

            return table;
        }

        private IEnumerable<Entity> ExecuteFetchRequest(IDictionary<string, IDictionary<Guid, Entity>> data)
        {
            // Create entities from the response array based on the FetchXML
            var fetchEntity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var results = GetFilteredResults(data, fetchEntity.name, null, fetchEntity.Items).ToList();

            var sorts = GetSorts(null, fetchEntity.Items);
            var sortedResults = (IEnumerable<Entity>) results;

            for (var i = 0; i < sorts.Count; i++)
            {
                var sort = sorts[i];

                if (i == 0)
                {
                    if (sort.Descending)
                        sortedResults = sortedResults.OrderByDescending(e => GetValue(e, sort.Attribute));
                    else
                        sortedResults = sortedResults.OrderBy(e => GetValue(e, sort.Attribute));
                }
                else
                {
                    if (sort.Descending)
                        sortedResults = ((IOrderedEnumerable<Entity>) sortedResults).ThenByDescending(e => GetValue(e, sort.Attribute));
                    else
                        sortedResults = ((IOrderedEnumerable<Entity>)sortedResults).ThenBy(e => GetValue(e, sort.Attribute));
                }
            }

            return sortedResults;
        }

        class Sort
        {
            public string Attribute { get; set; }

            public bool Descending { get; set; }
        }

        private List<Sort> GetSorts(string alias, object[] items)
        {
            if (items == null)
                return new List<Sort>();

            var sorts = items
                .OfType<FetchOrderType>()
                .Select(sort => new Sort { Attribute = sort.alias ?? ((alias == null ? "" : (alias + ".")) + sort.attribute), Descending = sort.descending })
                .ToList();

            foreach (var linkEntity in items.OfType<FetchLinkEntityType>())
                sorts.AddRange(GetSorts(linkEntity.alias, linkEntity.Items));

            return sorts;
        }

        private object GetValue(Entity entity, string attribute)
        {
            var value = entity[attribute];

            if (value is AliasedValue a)
                value = a.Value;

            return value;
        }

        private IEnumerable<Entity> GetFilteredResults(IDictionary<string, IDictionary<Guid, Entity>> data, string name, string alias, object[] items)
        {
            var results = new List<Entity>();

            if (!data.ContainsKey(name))
                return results;

            var joins = items == null ? new List<FetchLinkEntityType>() : items.OfType<FetchLinkEntityType>().ToList();

            if (joins.Count == 0)
            {
                results.AddRange(data[name].Values.Select(entity => GetAliasedEntity(entity, alias, items)));
            }
            else
            {
                // Get the results for each join
                var joinData = new List<ILookup<object, Entity>>();

                foreach (var join in joins)
                {
                    var joinResult = GetFilteredResults(data, join.name, join.alias, join.Items)
                        .Select(e => new { Key = GetJoinKey(e, join.alias, join.from), Entity = e })
                        .Where(kvp => kvp.Key != null)
                        .ToLookup(kvp => kvp.Key, kvp => kvp.Entity, new FetchEqualityComparer());

                    joinData.Add(joinResult);
                }

                foreach (var entity in data[name].Values)
                {
                    var joinResults = new List<List<Entity>>();

                    for (var i = 0; i < joins.Count; i++)
                    {
                        var join = joins[i];
                        var joinResult = joinData[i];
                        var joinKey = GetJoinKey(entity, null, join.to);

                        if (joinKey == null)
                        {
                            joinResults.Add(new List<Entity>());
                        }
                        else
                        {
                            joinResults.Add(joinResult[joinKey].ToList());
                        }
                    }

                    // Add each combination of joined results to the output
                    foreach (var joinedRecords in GetJoinCombinations(joins, joinResults))
                    {
                        var result = GetAliasedEntity(entity, alias, items);

                        foreach (var joinedRecord in joinedRecords)
                        {
                            if (joinedRecord == null)
                                continue;

                            foreach (var attr in joinedRecord.Attributes)
                                result[attr.Key] = attr.Value;
                        }

                        results.Add(result);
                    }
                }
            }

            // Apply filters
            if (items != null)
            {
                foreach (var filter in items.OfType<filter>())
                    results.RemoveAll(e => !IsMatch(e, filter, alias));
            }

            return results;
        }

        private object GetJoinKey(Entity e, string alias, string attrName)
        {
            if (alias != null)
                attrName = alias + "." + attrName;

            var value = e[attrName];

            if (value is AliasedValue av)
                value = av.Value;

            if (value is EntityReference er)
                value = er.Id;

            return value;
        }

        private IEnumerable<List<Entity>> GetJoinCombinations(List<FetchLinkEntityType> joins, List<List<Entity>> joinResults)
        {
            var joinIndexes = new int[joins.Count];

            while (IsValidJoinCombination(joins, joinResults, joinIndexes))
            {
                var combination = new List<Entity>();

                for (var i = 0; i < joins.Count; i++)
                {
                    if (joinResults[i].Count > joinIndexes[i])
                        combination.Add(joinResults[i][joinIndexes[i]]);
                    else
                        combination.Add(null);
                }

                yield return combination;

                for (var i = 0; i < joins.Count; i++)
                {
                    joinIndexes[i]++;

                    if (joinIndexes[i] < joinResults[i].Count)
                        break;

                    if (i < joins.Count - 1)
                        joinIndexes[i] = 0;
                }
            }
        }

        private bool IsValidJoinCombination(List<FetchLinkEntityType> joins, List<List<Entity>> joinResults, int[] joinIndexes)
        {
            for (var i = 0; i < joins.Count; i++)
            {
                if (joinIndexes[i] == 0 && joinResults[i].Count == 0 && joins[i].linktype == "outer")
                    continue;

                if (joinIndexes[i] >= joinResults[i].Count)
                    return false;
            }

            return true;
        }

        private Entity GetAliasedEntity(Entity entity, string alias, object[] items)
        {
            var result = new Entity(entity.LogicalName, entity.Id);

            foreach (var attr in entity.Attributes)
            {
                var name = attr.Key;
                var value = attr.Value;

                if (alias != null)
                {
                    name = alias + "." + name;

                    if (value != null && !(value is AliasedValue))
                        value = new AliasedValue(entity.LogicalName, attr.Key, value);
                }

                result[name] = value;
            }

            if (items != null)
            {
                foreach (var aliasedAttribute in items.OfType<FetchAttributeType>().Where(a => a.alias != null))
                {
                    var value = result[aliasedAttribute.name];

                    if (!(value is AliasedValue))
                        value = new AliasedValue(entity.LogicalName, aliasedAttribute.name, value);

                    result[aliasedAttribute.alias] = value;
                }
            }

            return result;
        }

        private bool IsMatch(Entity entity, filter filter, string alias)
        {
            foreach (var condition in filter.Items.OfType<condition>())
            {
                var conditionMatch = IsMatch(entity, condition, alias);

                if (filter.type == filterType.and && !conditionMatch)
                    return false;
                else if (filter.type == filterType.or && conditionMatch)
                    return true;
            }

            foreach (var subFilter in filter.Items.OfType<filter>())
            {
                var filterMatch = IsMatch(entity, subFilter, alias);

                if (filter.type == filterType.and && !filterMatch)
                    return false;
                else if (filter.type == filterType.or && filterMatch)
                    return true;
            }

            if (filter.type == filterType.and)
                return true;

            return false;
        }

        private bool IsMatch(Entity entity, condition condition, string alias)
        {
            var attribute = condition.attribute;

            if (!String.IsNullOrEmpty(condition.entityname))
                attribute = condition.entityname + "." + condition.attribute;
            else if (!String.IsNullOrEmpty(alias))
                attribute = alias + "." + condition.attribute;

            var actualValue = entity[attribute];
            var expectedValue = (object) condition.value;

            if (!String.IsNullOrEmpty(condition.valueof))
                expectedValue = entity[condition.valueof];

            if (actualValue is AliasedValue a1)
                actualValue = a1.Value;

            if (expectedValue is AliasedValue a2)
                expectedValue = a2.Value;

            if (actualValue == null)
                return condition.@operator == @operator.@null;

            if (actualValue != null && expectedValue != null)
                expectedValue = ChangeType(expectedValue, actualValue.GetType());

            switch (condition.@operator)
            {
                case @operator.eq:
                    return IsEqual(actualValue, expectedValue);

                case @operator.@null:
                    return actualValue == null;

                case @operator.beginswith:
                    return ((string)actualValue).StartsWith((string)expectedValue, StringComparison.OrdinalIgnoreCase);

                case @operator.endswith:
                    return ((string)actualValue).EndsWith((string)expectedValue, StringComparison.OrdinalIgnoreCase);

                case @operator.ge:
                    return Convert.ToDecimal(actualValue) >= Convert.ToDecimal(expectedValue);

                case @operator.gt:
                    return Convert.ToDecimal(actualValue) > Convert.ToDecimal(expectedValue);

                case @operator.@in:
                    return condition.Items
                        .Select(i => ChangeType(i.Value, actualValue.GetType()))
                        .Any(i => IsEqual(actualValue, i));

                case @operator.le:
                    return Convert.ToDecimal(actualValue) <= Convert.ToDecimal(expectedValue);

                case @operator.lt:
                    return Convert.ToDecimal(actualValue) < Convert.ToDecimal(expectedValue);

                case @operator.like:
                    return ExpressionFunctions.Like((string) actualValue, (string) expectedValue);

                case @operator.ne:
                case @operator.neq:
                    return !IsEqual(actualValue, expectedValue);

                case @operator.notbeginwith:
                    return !((string)actualValue).StartsWith((string)expectedValue, StringComparison.OrdinalIgnoreCase);

                case @operator.notendwith:
                    return !((string)actualValue).EndsWith((string)expectedValue, StringComparison.OrdinalIgnoreCase);

                case @operator.notin:
                    return !condition.Items
                        .Select(i => ChangeType(i.Value, actualValue.GetType()))
                        .Any(i => IsEqual(actualValue, i));

                case @operator.notlike:
                    return !ExpressionFunctions.Like((string)actualValue, (string)expectedValue);

                case @operator.notnull:
                    return actualValue != null;

                default:
                    throw new NotSupportedException();
            }
        }

        private static bool IsEqual(object x, object y)
        {
            if (x == null || y == null)
                return false;

            if (x is string xs && y is string ys)
                return xs.Equals(ys, StringComparison.OrdinalIgnoreCase);

            return x.Equals(y);
        }

        protected static object ChangeType(object value, Type type)
        {
            if (type == typeof(bool) && value is string boolStr && (boolStr == "1" || boolStr == "0"))
                return boolStr == "1";

            if (type.IsEnum && value is string enumStr)
                return Enum.Parse(type, enumStr);

            return Convert.ChangeType(value, type);
        }

        class FetchEqualityComparer : IEqualityComparer<object>
        {
            public new bool Equals(object x, object y)
            {
                return IsEqual(x, y);
            }

            public int GetHashCode(object obj)
            {
                if (obj is string s)
                    return s.ToLowerInvariant().GetHashCode();

                return obj.GetHashCode();
            }
        }
    }

    /// <summary>
    /// A SELECT query to return details of global optionset metadata
    /// </summary>
    public class GlobalOptionSetQuery : MetadataQuery<RetrieveAllOptionSetsRequest, RetrieveAllOptionSetsResponse>
    {
        public GlobalOptionSetQuery()
        {
            Request = new RetrieveAllOptionSetsRequest();
        }

        protected override IDictionary<string, IDictionary<Guid, Entity>> GetData(RetrieveAllOptionSetsResponse response) => MetaMetadata.GetData(response.OptionSetMetadata);
    }

    /// <summary>
    /// A SELECT query to return details of entity metadata
    /// </summary>
    public class EntityMetadataQuery : MetadataQuery<RetrieveMetadataChangesRequest, RetrieveMetadataChangesResponse>
    {
        public EntityMetadataQuery()
        {
            Request = new RetrieveMetadataChangesRequest();
        }

        class RequiredEntity
        {
            private bool _required;

            public RequiredEntity(string logicalName, params RequiredEntity[] children)
            {
                LogicalName = logicalName;
                Children = children;
            }

            public string LogicalName { get; }

            public RequiredEntity[] Children { get; }

            public bool Required
            {
                get { return _required || Children.Any(c => c.Required); }
                set { _required = true; }
            }

            public void SetRequired(IDictionary<string, List<object>> itemsByEntity)
            {
                if (itemsByEntity.ContainsKey(LogicalName))
                    Required = true;

                var meta = MetaMetadata.GetMetadata().Single(m => m.LogicalName == LogicalName);
                var derivedTypes = MetaMetadata.GetMetadata().Where(m => meta.Type.IsAssignableFrom(m.Type)).Select(m => m.LogicalName);

                foreach (var derivedType in derivedTypes)
                {
                    if (itemsByEntity.ContainsKey(derivedType))
                        Required = true;
                }

                foreach (var child in Children)
                    child.SetRequired(itemsByEntity);
            }

            public bool IsRequired(string logicalName)
            {
                if (logicalName == LogicalName)
                    return Required;

                return Children.Any(c => c.IsRequired(logicalName));
            }
        }

        public override void FinalizeRequest(IOrganizationService org)
        {
            // If there are conditions in a root-level filter that can be promoted to an inner-joined link entity, do so now to
            // generate a more efficient filter later on.
            MoveConditionsToLinkEntity();

            // Build a tree to identify what higher level objects are required to retrieve lower level ones
            var tree = new RequiredEntity("entity",
                new RequiredEntity("attribute",
                    new RequiredEntity("label",
                        new RequiredEntity("localizedlabel")
                    )
                ),
                new RequiredEntity("relationship_1_n",
                    new RequiredEntity("label",
                        new RequiredEntity("localizedlabel")
                    )
                ),
                new RequiredEntity("relationship_n_n",
                    new RequiredEntity("label",
                        new RequiredEntity("localizedlabel")
                    )
                ),
                new RequiredEntity("label",
                    new RequiredEntity("localizedlabel")
                )
            );

            var itemsByEntity = GetItemsByEntity();

            tree.SetRequired(itemsByEntity);

            Request.Query = new EntityQueryExpression();
            Request.Query.Properties = new MetadataPropertiesExpression(GetPropertyNames(itemsByEntity, "entity", tree));
            Request.Query.Criteria = GetFilter(itemsByEntity, "entity");

            if (tree.IsRequired("attribute"))
            {
                Request.Query.AttributeQuery = new AttributeQueryExpression();
                Request.Query.AttributeQuery.Properties = new MetadataPropertiesExpression(GetPropertyNames(itemsByEntity, "attribute", tree));
                Request.Query.AttributeQuery.Criteria = GetFilter(itemsByEntity, "attribute");
            }

            if (tree.IsRequired("relationship_1_n") || tree.IsRequired("relationship_n_n"))
            {
                Request.Query.RelationshipQuery = new RelationshipQueryExpression();
                Request.Query.RelationshipQuery.Properties = new MetadataPropertiesExpression(GetPropertyNames(itemsByEntity, "relationship_1_n", tree).Union(GetPropertyNames(itemsByEntity, "relationship_n_n", tree)).ToArray());
                Request.Query.RelationshipQuery.Criteria = GetFilter(itemsByEntity, "relationship_1_n");
            }

            if (!itemsByEntity.ContainsKey("localizedlabel"))
            {
                var qry = new QueryExpression("usersettings");
                qry.TopCount = 1;
                qry.ColumnSet = new ColumnSet("localeid");
                qry.Criteria.AddCondition("systemuserid", ConditionOperator.EqualUserId);
                var userLink = qry.AddLink("systemuser", "systemuserid", "systemuserid");
                var orgLink = userLink.AddLink("organization", "organizationid", "organizationid");
                orgLink.EntityAlias = "org";
                orgLink.Columns = new ColumnSet("localeid");
                var locale = org.RetrieveMultiple(qry).Entities.Single();

                int localeId;

                if (locale.Contains("localeid"))
                    localeId = locale.GetAttributeValue<int>("localeid");
                else
                    localeId = locale.GetAliasedAttributeValue<int>("org.localeid");

                Request.Query.LabelQuery = new LabelQueryExpression();
                Request.Query.LabelQuery.FilterLanguages.Add(localeId);
            }
        }

        private void MoveConditionsToLinkEntity()
        {
            if (!IsFilterValid)
                return;

            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            var filter = entity.Items.OfType<filter>().SingleOrDefault();

            if (filter == null || filter.type != filterType.and)
                return;

            var joinedConditions = filter.Items
                .OfType<condition>()
                .Where(c => !String.IsNullOrEmpty(c.entityname))
                .ToList();

            foreach (var condition in joinedConditions)
            {
                var join = entity.Items.OfType<FetchLinkEntityType>().SingleOrDefault(j => j.alias == condition.entityname);

                if (join == null)
                    continue;

                if (join.linktype != "inner")
                    continue;

                condition.entityname = null;
                filter.Items = filter.Items.Except(new[] { condition }).ToArray();

                var joinFilter = join.Items.OfType<filter>().SingleOrDefault();

                if (joinFilter == null)
                {
                    joinFilter = new filter();
                    joinFilter.Items = Array.Empty<object>();
                    joinFilter.type = filterType.and;
                    join.Items = join.Items.Concat(new[] { joinFilter }).ToArray();
                }

                joinFilter.Items = joinFilter.Items.Concat(new[] { condition }).ToArray();
            }
        }

        private MetadataFilterExpression GetFilter(IDictionary<string, List<object>> itemsByEntity, string entity)
        {
            if (!IsFilterValid)
                return null;

            if (!itemsByEntity.TryGetValue(entity, out var items))
            {
                if (entity == "attribute")
                {
                    foreach (var attributeType in GetAttributeTypes())
                    {
                        if (itemsByEntity.TryGetValue(attributeType, out var attributeItems))
                        {
                            if (items == null)
                            {
                                items = attributeItems;
                                entity = attributeType;
                            }
                            else
                            {
                                return null;
                            }
                        }
                    }
                }

                if (items == null)
                    return null;
            }

            var filters = items.OfType<filter>().ToList();

            if (filters.Count != 1)
                return null;

            if (!ConvertFilter(entity, filters[0], out var filter))
                return null;

            return filter;
        }

        private bool IsFilterValid
        {
            get
            {
                // We can only apply a filter on the metadata if it is used by the FetchXML query in the same heirarchy as it is returned
                // by the metadata query
                var entity = FetchXml.Items.OfType<FetchEntityType>().Single();

                return IsEntityFilterValid(entity.name, entity.Items);
            }
        }

        private bool IsEntityFilterValid(string entity, object[] items)
        {
            if (entity == "entity")
            {
                foreach (var join in items.OfType<FetchLinkEntityType>())
                {
                    if (join.name == "label" || join.name == "localizedlabel")
                        continue;

                    if (IsAttributeType(join.name) && join.from == "entitylogicalname" && join.to == "logicalname")
                        continue;

                    if (join.name == "relationship_1_n" && join.from == "referencedentity" && join.to == "logicalname")
                        continue;

                    if (join.name == "relationship_1_n" && join.from == "referencingentity" && join.to == "logicalname")
                        continue;

                    if (join.name == "relationship_n_n" && join.from == "entity1logicalname" && join.to == "logicalname")
                        continue;

                    if (join.name == "relationship_n_n" && join.from == "entity2logicalname" && join.to == "logicalname")
                        continue;

                    return false;
                }
            }

            if (IsAttributeType(entity))
            {
                foreach (var join in items.OfType<FetchLinkEntityType>())
                {
                    if (join.name == "label" || join.name == "localizedlabel")
                        continue;

                    if (IsAttributeType(join.name) && (join.from == "metadataid" || join.from == join.name + "id") && (join.to == "metadataid" || join.to == entity + "id"))
                        continue;

                    if (join.name == "entity" && join.from == "logicalname" && join.to == "entitylogicalname")
                        continue;

                    return false;
                }
            }

            if (entity == "relationship_1_n")
            {
                foreach (var join in items.OfType<FetchLinkEntityType>())
                {
                    if (join.name == "label" || join.name == "localizedlabel")
                        continue;

                    if (join.name == "entity" && join.from == "logicalname" && (join.to == "referencedentity" || join.to == "referencingentity"))
                        continue;

                    return false;
                }
            }

            if (entity == "relationship_n_n")
            {
                foreach (var join in items.OfType<FetchLinkEntityType>())
                {
                    if (join.name == "label" || join.name == "localizedlabel")
                        continue;

                    if (join.name == "entity" && join.from == "logicalname" && (join.to == "entity1logicalname" || join.to == "entity2logicalname"))
                        continue;

                    return false;
                }
            }

            foreach (var join in items.OfType<FetchLinkEntityType>())
            {
                if (!IsEntityFilterValid(join.name, join.Items))
                    return false;
            }

            return true;
        }

        private bool ConvertFilter(string entity, filter filter, out MetadataFilterExpression converted)
        {
            var mmd = MetaMetadata.GetMetadata().Single(md => md.LogicalName == entity);
            var metadata = mmd.GetEntityMetadata();
            var type = mmd.Type;

            converted = new MetadataFilterExpression(filter.type == filterType.and ? LogicalOperator.And : LogicalOperator.Or);

            // If we're expecting a specific type of attribute, enforce this with a condition
            if (typeof(AttributeMetadata).IsAssignableFrom(type) && type != typeof(AttributeMetadata))
            {
                var typeCondition = new MetadataConditionExpression
                {
                    PropertyName = nameof(AttributeMetadata.AttributeType),
                    ConditionOperator = MetadataConditionOperator.Equals,
                    Value = ((AttributeMetadata)Activator.CreateInstance(type)).AttributeType
                };

                converted.Conditions.Add(typeCondition);
            }

            foreach (var condition in filter.Items.OfType<condition>())
            {
                if (!String.IsNullOrEmpty(condition.entityname))
                {
                    if (filter.type == filterType.or)
                        return false;

                    continue;
                }

                if (!String.IsNullOrEmpty(condition.valueof))
                {
                    if (filter.type == filterType.or)
                        return false;

                    continue;
                }

                var attribute = metadata.Attributes.Single(a => a.LogicalName == condition.attribute);
                var prop = type.GetProperty(attribute.SchemaName);
                var value = condition.value;

                if (prop.PropertyType == typeof(Label))
                {
                    if (filter.type == filterType.or)
                        return false;

                    continue;
                }

                if (typeof(AttributeMetadata).IsAssignableFrom(type) && prop.DeclaringType != typeof(AttributeMetadata))
                {
                    // Can only filter on properties of the base attribute type, not derived types.
                    continue;
                }

                var convertedCondition = new MetadataConditionExpression();
                convertedCondition.PropertyName = attribute.SchemaName;
                var propType = prop.PropertyType;
                if (propType.IsGenericType && propType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    propType = propType.GetGenericArguments()[0];

                switch (condition.@operator)
                {
                    case @operator.@null:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.Equals;
                        convertedCondition.Value = null;
                        break;

                    case @operator.notnull:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.NotEquals;
                        convertedCondition.Value = null;
                        break;

                    case @operator.eq:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.Equals;
                        convertedCondition.Value = ChangeType(condition.value, propType);
                        break;

                    case @operator.ne:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.NotEquals;
                        convertedCondition.Value = ChangeType(condition.value, propType);
                        break;

                    case @operator.lt:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.LessThan;
                        convertedCondition.Value = ChangeType(condition.value, propType);
                        break;

                    case @operator.gt:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.GreaterThan;
                        convertedCondition.Value = ChangeType(condition.value, propType);
                        break;

                    case @operator.@in:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.In;
                        convertedCondition.Value = condition.Items.Select(i => ChangeType(i.Value, propType)).ToArray();
                        break;

                    case @operator.notin:
                        convertedCondition.ConditionOperator = MetadataConditionOperator.NotIn;
                        convertedCondition.Value = condition.Items.Select(i => ChangeType(i.Value, propType)).ToArray();
                        break;

                    default:
                        if (filter.type == filterType.or)
                            return false;

                        continue;
                }

                converted.Conditions.Add(convertedCondition);
            }

            foreach (var subFilter in filter.Items.OfType<filter>())
            {
                if (ConvertFilter(entity, subFilter, out var convertedSubFilter))
                    converted.Filters.Add(convertedSubFilter);
                else if (filter.type == filterType.or)
                    return false;
            }

            return true;
        }

        private IDictionary<string, List<object>> GetItemsByEntity()
        {
            var itemsByEntity = new Dictionary<string, List<object>>();
            var entity = FetchXml.Items.OfType<FetchEntityType>().Single();
            AddItems(itemsByEntity, entity.name, entity.Items);
            return itemsByEntity;
        }

        private void AddItems(IDictionary<string, List<object>> itemsByEntity, string entity, object[] items)
        {
            if (!itemsByEntity.TryGetValue(entity, out var list))
            {
                list = new List<object>();
                itemsByEntity[entity] = list;
            }

            if (items == null)
                return;

            list.AddRange(items);

            foreach (var join in items.OfType<FetchLinkEntityType>())
            {
                AddItems(itemsByEntity, join.name, join.Items);
                AddItems(itemsByEntity, entity, new object[] { new FetchAttributeType { name = join.to } });
                AddItems(itemsByEntity, join.name, new object[] { new FetchAttributeType { name = join.from } });
            }
        }

        private string[] GetPropertyNames(IDictionary<string, List<object>> itemsByEntity, string entity, RequiredEntity tree)
        {
            var propNames = new List<string>();

            if (!itemsByEntity.TryGetValue(entity, out var items))
                items = new List<object>();
            
            var allAttributes = items.OfType<allattributes>().Any();
            var attributes = items.OfType<FetchAttributeType>().Select(a => a.name)
                .Union(items.OfType<FetchOrderType>().Where(a => !String.IsNullOrEmpty(a.attribute)).Select(a => a.attribute))
                .Union(GetConditions(items.OfType<filter>()).Where(c => String.IsNullOrEmpty(c.entityname)).Select(c => c.attribute))
                .ToArray();

            var metadata = MetaMetadata.GetMetadata().Single(md => md.LogicalName == entity);

            foreach (var prop in metadata.Type.GetProperties())
            {
                if (!prop.CanRead)
                    continue;

                if (prop.DeclaringType == typeof(MetadataBase) && prop.Name == nameof(MetadataBase.ExtensionData))
                    continue;

                if (allAttributes ||
                    attributes.Contains(prop.Name.ToLower()) ||
                    attributes.Contains(prop.Name.ToLower() + "id") ||
                    (prop.Name == nameof(MetadataBase.MetadataId) && attributes.Contains(metadata.LogicalName + "id")) ||
                    (entity == "entity" && prop.Name == nameof(EntityMetadata.Attributes) && tree.IsRequired("attribute")) ||
                    (entity == "entity" && prop.Name == nameof(EntityMetadata.OneToManyRelationships) && tree.IsRequired("relationship_1_n")) ||
                    (entity == "entity" && prop.Name == nameof(EntityMetadata.ManyToOneRelationships) && tree.IsRequired("relationship_1_n")) ||
                    (entity == "entity" && prop.Name == nameof(EntityMetadata.ManyToManyRelationships) && tree.IsRequired("relationship_n_n")) ||
                    (prop.PropertyType == typeof(Label) && tree.IsRequired("label")))
                {
                    propNames.Add(prop.Name);
                }
            }

            if (entity == "attribute")
            {
                foreach (var attributeType in GetAttributeTypes())
                    propNames.AddRange(GetPropertyNames(itemsByEntity, attributeType, tree));
            }

            return propNames.ToArray();
        }

        private IEnumerable<condition> GetConditions(IEnumerable<filter> filters)
        {
            foreach (var filter in filters)
            {
                if (filter.Items == null)
                    continue;

                foreach (var childCondition in GetConditions(filter.Items.OfType<filter>()))
                    yield return childCondition;

                foreach (var condition in filter.Items.OfType<condition>())
                    yield return condition;
            }
        }

        private bool IsAttributeType(string logicalName)
        {
            if (logicalName == "attribute")
                return true;

            return GetAttributeTypes().Contains(logicalName);
        }

        private IEnumerable<string> GetAttributeTypes()
        {
            return MetaMetadata.GetMetadata()
                .Where(meta => typeof(AttributeMetadata).IsAssignableFrom(meta.Type) && meta.Type != typeof(AttributeMetadata))
                .Select(meta => meta.LogicalName);
        }

        protected override IDictionary<string, IDictionary<Guid, Entity>> GetData(RetrieveMetadataChangesResponse response) => MetaMetadata.GetData(response.EntityMetadata.ToArray());
    }
}
