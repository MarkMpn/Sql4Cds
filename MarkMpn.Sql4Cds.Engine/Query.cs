using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Messages;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// A CDS query that can be executed
    /// </summary>
    /// <remarks>
    /// Further details for specific query types can be found from one of the derived types, but all query types can be executed
    /// using the same <see cref="Execute(IOrganizationService, IAttributeMetadataCache, IQueryExecutionOptions)"/> method.
    /// </remarks>
    [Obsolete("Use IRootExecutionPlanNode instead")]
    public abstract class Query
    {
        /// <summary>
        /// The original SQL that the query was created from
        /// </summary>
        public string Sql { get; internal set; }

        /// <summary>
        /// The location in the original parsed string that the query started at
        /// </summary>
        public int Index { get; internal set; }

        /// <summary>
        /// The length of the original SQL command
        /// </summary>
        public int Length { get; internal set; }

        /// <summary>
        /// The T-SQL that can be executed against the TDS endpoint, if available
        /// </summary>
        public string TSql { get; internal set; }
        
        internal IRootExecutionPlanNode Node { get; set; }

        /// <summary>
        /// Executes the query
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to execute the query against</param>
        /// <param name="metadata">The metadata cache to use when executing the query</param>
        /// <param name="options">The options to apply to the query execution</param>
        /// <remarks>
        /// After calling this method, the results can be retrieved from the <see cref="Result"/> property.
        /// </remarks>
        public void Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options)
        {
            try
            {
                if (Node is IDataSetExecutionPlanNode dataSet)
                    Result = DataTableToEntityCollection(dataSet.Execute(dataSources, options, null, null));
                else if (Node is IDmlQueryExecutionPlanNode dml)
                    Result = dml.Execute(dataSources, options, null, null);
                else
                    throw new ApplicationException("Unexpected execution plan node type");
            }
            catch (Exception ex)
            {
                Result = ex;
            }
        }

        private EntityCollection DataTableToEntityCollection(DataTable table)
        {
            var entityCollection = new EntityCollection();

            foreach (DataRow row in table.Rows)
            {
                var entity = new Entity();

                foreach (DataColumn col in table.Columns)
                {
                    var value = row[col];

                    if (value is INullable nullable)
                    {
                        if (nullable.IsNull)
                            value = null;
                        else
                            value = value.GetType().GetProperty("Value").GetValue(value);
                    }

                    entity[col.ColumnName] = value;
                }

                entityCollection.Entities.Add(entity);
            }

            return entityCollection;
        }

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
    [Obsolete("Use IRootExecutionPlanNode instead")]
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
                FetchXmlString = FetchXmlScan.Serialize(_fetch);
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
        /// Indicates if this query uses the <see cref="FetchType.distinct"/> option without having a sort order specified
        /// </summary>
        public bool DistinctWithoutSort { get; private set; }
    }

    /// <summary>
    /// A SELECT query to return data using FetchXML
    /// </summary>
    [Obsolete("Use IDataSetExecutionPlanNode instead")]
    public class SelectQuery : FetchXmlQuery
    {
    }

    /// <summary>
    /// An UPDATE query to identify records via FetchXML and change them
    /// </summary>
    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
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

        /// <summary>
        /// Indicates if a WHERE clause was applied to the query
        /// </summary>
        public bool HasWhere { get; set; }
    }

    /// <summary>
    /// A DELETE query to identify records via FetchXML and delete them
    /// </summary>
    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
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

        /// <summary>
        /// Indicates if a WHERE clause was applied to the query
        /// </summary>
        public bool HasWhere { get; set; }
    }

    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
    public abstract class InsertQuery : Query
    {
        /// <summary>
        /// The logical name of the entity to add
        /// </summary>
        public string LogicalName { get; set; }
    }

    /// <summary>
    /// An INSERT query to add fixed values
    /// </summary>
    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
    public class InsertValues : InsertQuery
    {
        /// <summary>
        /// A list of records to insert
        /// </summary>
        public IDictionary<string, object>[] Values { get; set; }
    }

    /// <summary>
    /// An INSERT query to add records based on the results of a FetchXML query
    /// </summary>
    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
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
    }

    /// <summary>
    /// Indicates that the query requires some additional processing to generate the final query after the conversion from FetchXML
    /// </summary>
    public interface IQueryRequiresFinalization
    {
        void FinalizeRequest(IOrganizationService org, IQueryExecutionOptions options);
    }

    /// <summary>
    /// A SELECT request that retrieves metadata using a specialized request
    /// </summary>
    /// <typeparam name="TRequest"></typeparam>
    /// <typeparam name="TResponse"></typeparam>
    [Obsolete("Use IDataSetExecutionPlanNode instead")]
    public abstract class MetadataQuery<TRequest, TResponse> : SelectQuery, IQueryRequiresFinalization
        where TRequest : OrganizationRequest
        where TResponse : OrganizationResponse
    {
        /// <summary>
        /// Returns or sets the metadata request to execute
        /// </summary>
        public TRequest Request { get; set; }

        /// <summary>
        /// Applies any final changes to the <see cref="Request"/> before it is executed
        /// </summary>
        /// <param name="org">The organization service that will execute the request</param>
        /// <param name="options">The options to control the execution of the query</param>
        public virtual void FinalizeRequest(IOrganizationService org, IQueryExecutionOptions options)
        {
        }
    }

    /// <summary>
    /// A SELECT query to return details of global optionset metadata
    /// </summary>
    [Obsolete("Use IDataSetExecutionPlanNode instead")]
    public class GlobalOptionSetQuery : MetadataQuery<RetrieveAllOptionSetsRequest, RetrieveAllOptionSetsResponse>
    {
        public GlobalOptionSetQuery()
        {
            Request = new RetrieveAllOptionSetsRequest();
        }
    }

    /// <summary>
    /// A SELECT query to return details of entity metadata
    /// </summary>
    [Obsolete("Use IDataSetExecutionPlanNode instead")]
    public class EntityMetadataQuery : MetadataQuery<RetrieveMetadataChangesRequest, RetrieveMetadataChangesResponse>
    {
        public EntityMetadataQuery()
        {
            Request = new RetrieveMetadataChangesRequest();
        }
    }

    /// <summary>
    /// EXECUTE AS [USER | LOGIN] query
    /// </summary>
    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
    public class ImpersonateQuery : FetchXmlQuery
    {
        public ImpersonateQuery(string username)
        {
            Username = username;
        }

        public string Username { get; }
    }

    /// <summary>
    /// REVERT query
    /// </summary>
    [Obsolete("Use IDmlQueryExecutionPlanNode instead")]
    public class RevertQuery : Query
    {
    }
}
