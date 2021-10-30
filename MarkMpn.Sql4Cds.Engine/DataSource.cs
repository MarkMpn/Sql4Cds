using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Holds all the required information about the connection to an instance
    /// </summary>
    public class DataSource
    {
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
