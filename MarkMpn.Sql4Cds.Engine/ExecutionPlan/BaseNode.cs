using System;
using System.Collections.Generic;
using System.ComponentModel;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk.Metadata;
using Newtonsoft.Json;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// A base class for all execution plan nodes
    /// </summary>
    abstract class BaseNode : IExecutionPlanNode
    {
        /// <summary>
        /// The parent of this node
        /// </summary>
        [Browsable(false)]
        [JsonIgnore]
        public IExecutionPlanNode Parent { get; set; }

        /// <summary>
        /// The number of times this node has been executed
        /// </summary>
        [Category("Statistics")]
        [Description("The number of times this node has been executed")]
        [DisplayName("Execution Count")]
        [BrowsableInEstimatedPlan(false)]
        public abstract int ExecutionCount { get; }

        /// <summary>
        /// The total time this node has taken, including the time of any child nodes
        /// </summary>
        [Category("Statistics")]
        [Description("The total time this node has taken, including the time of any child nodes")]
        [BrowsableInEstimatedPlan(false)]
        public abstract TimeSpan Duration { get; }

        /// <summary>
        /// Retrieves a list of child nodes
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<IExecutionPlanNode> GetSources();

        /// <summary>
        /// Adds columns to the data source that are required by parent nodes
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <param name="requiredColumns">The names of columns that are required by the parent node</param>
        public abstract void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns);

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
        /// Notifies the node that query folding is complete
        /// </summary>
        public virtual void FinishedFolding(NodeCompilationContext context)
        {
        }

        public override string ToString()
        {
            return System.Text.RegularExpressions.Regex.Replace(GetType().Name.Replace("Node", ""), "([a-z])([A-Z])", "$1 $2");
        }
    }
}
