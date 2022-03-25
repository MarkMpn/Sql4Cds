using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Query;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Holds all the required information about the connection to an instance
    /// </summary>
    public class DataSource
    {
        /// <summary>
        /// Creates a new <see cref="DataSource"/> using default values based on an existing connection.
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> that provides the connection to the instance</param>
        public DataSource(IOrganizationService org)
        {
            string name;

#if NETCOREAPP
            if (org is ServiceClient svc)
            {
                name = svc.ConnectedOrgUniqueName;
            }
#else
            if (org is CrmServiceClient svc)
            {
                name = svc.ConnectedOrgUniqueName;
            }
#endif
            else
            {
                var orgDetails = org.RetrieveMultiple(new QueryExpression("organization") { ColumnSet = new ColumnSet("name") }).Entities[0];
                name = orgDetails.GetAttributeValue<string>("name");
            }

            Connection = org;
            Metadata = new AttributeMetadataCache(org);
            Name = name;
            TableSizeCache = new TableSizeCache(org, Metadata);
        }

        /// <summary>
        /// Creates a new <see cref="DataSource"/>
        /// </summary>
        public DataSource()
        {
        }

        /// <summary>
        /// The identifier for this instance
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// The connection to the instance to use to retrieve or modify data
        /// </summary>
        public IOrganizationService Connection { get; set; }

        /// <summary>
        /// A cache of the metadata of the instance
        /// </summary>
        public IAttributeMetadataCache Metadata { get; set; }

        /// <summary>
        /// A cache of the number of records in each table in the instance
        /// </summary>
        public ITableSizeCache TableSizeCache { get; set; }
    }
}
