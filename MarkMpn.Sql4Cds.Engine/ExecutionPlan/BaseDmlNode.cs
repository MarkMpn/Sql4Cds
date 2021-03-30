using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A base class for execution plan nodes that implement a DML operation
    /// </summary>
    abstract class BaseDmlNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        /// <summary>
        /// Temporarily applies global settings to improve the performance of parallel operations
        /// </summary>
        class ParallelConnectionSettings : IDisposable
        {
            private readonly int _connectionLimit;
            private readonly int _threadPoolThreads;
            private readonly int _iocpThreads;
            private readonly bool _expect100Continue;
            private readonly bool _useNagleAlgorithm;

            public ParallelConnectionSettings()
            {
                // Store the current settings
                _connectionLimit = System.Net.ServicePointManager.DefaultConnectionLimit;
                ThreadPool.GetMinThreads(out _threadPoolThreads, out _iocpThreads);
                _expect100Continue = System.Net.ServicePointManager.Expect100Continue;
                _useNagleAlgorithm = System.Net.ServicePointManager.UseNagleAlgorithm;

                // Apply the required settings
                System.Net.ServicePointManager.DefaultConnectionLimit = 65000;
                ThreadPool.SetMinThreads(100, 100);
                System.Net.ServicePointManager.Expect100Continue = false;
                System.Net.ServicePointManager.UseNagleAlgorithm = false;
            }

            public void Dispose()
            {
                // Restore the original settings
                System.Net.ServicePointManager.DefaultConnectionLimit = _connectionLimit;
                ThreadPool.SetMinThreads(_threadPoolThreads, _iocpThreads);
                System.Net.ServicePointManager.Expect100Continue = _expect100Continue;
                System.Net.ServicePointManager.UseNagleAlgorithm = _useNagleAlgorithm;
            }
        }

        /// <summary>
        /// The SQL string that the query was converted from
        /// </summary>
        [Browsable(false)]
        public string Sql { get; set; }

        /// <summary>
        /// The position of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Index { get; set; }

        /// <summary>
        /// The length of the SQL query within the entire query text
        /// </summary>
        [Browsable(false)]
        public int Length { get; set; }

        /// <summary>
        /// Changes system settings to optimise for parallel connections
        /// </summary>
        /// <returns>An object to dispose of to reset the settings to their original values</returns>
        protected IDisposable UseParallelConnections() => new ParallelConnectionSettings();

        /// <summary>
        /// Gets the name to show for an entity
        /// </summary>
        /// <param name="count">The number of records to indicate if the singular or plural name should be returned</param>
        /// <param name="meta">The metadata for the entity</param>
        /// <returns>The name to show for the entity</returns>
        protected string GetDisplayName(int count, EntityMetadata meta)
        {
            if (count == 1)
                return meta.DisplayName?.UserLocalizedLabel?.Label ?? meta.LogicalName;

            return meta.DisplayCollectionName?.UserLocalizedLabel?.Label ??
                meta.LogicalCollectionName ??
                meta.LogicalName;
        }

        /// <summary>
        /// Executes the DML query and returns an appropriate log message
        /// </summary>
        /// <param name="org">The <see cref="IOrganizationService"/> to use to get the data</param>
        /// <param name="metadata">The <see cref="IAttributeMetadataCache"/> to use to get metadata</param>
        /// <param name="options"><see cref="IQueryExpressionVisitor"/> to indicate how the query can be executed</param>
        /// <param name="parameterTypes">A mapping of parameter names to their related types</param>
        /// <param name="parameterValues">A mapping of parameter names to their current values</param>
        /// <returns>A log message to display</returns>
        public abstract string Execute(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues);

        /// <summary>
        /// Attempts to fold this node into its source to simplify the query
        /// </summary>
        /// <param name="metadata">The <see cref="IAttributeMetadataCache"/> to use to get metadata</param>
        /// <param name="options"><see cref="IQueryExpressionVisitor"/> to indicate how the query can be executed</param>
        /// <param name="parameterTypes">A mapping of parameter names to their related types</param>
        /// <returns>The node that should be used in place of this node</returns>
        public abstract IRootExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes);
    }
}
