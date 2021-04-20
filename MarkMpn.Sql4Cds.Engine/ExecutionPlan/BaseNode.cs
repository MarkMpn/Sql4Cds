using System;
using System.Collections.Generic;
using System.ComponentModel;

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
        public IExecutionPlanNode Parent { get; set; }

        /// <summary>
        /// The number of times this node has been executed
        /// </summary>
        [Category("Statistics")]
        [Description("The number of times this node has been executed")]
        [DisplayName("Execution Count")]
        public abstract int ExecutionCount { get; }

        /// <summary>
        /// The total time this node has taken, including the time of any child nodes
        /// </summary>
        [Category("Statistics")]
        [Description("The total time this node has taken, including the time of any child nodes")]
        public abstract TimeSpan Duration { get; }

        /// <summary>
        /// Retrieves a list of child nodes
        /// </summary>
        /// <returns></returns>
        public abstract IEnumerable<IExecutionPlanNode> GetSources();

        /// <summary>
        /// Adds columns to the data source that are required by parent nodes
        /// </summary>
        /// <param name="metadata">The <see cref="IAttributeMetadataCache"/> to use to get metadata</param>
        /// <param name="parameterTypes">A mapping of parameter names to their related types</param>
        /// <param name="requiredColumns">The names of columns that are required by the parent node</param>
        public abstract void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns);

        public override string ToString()
        {
            return System.Text.RegularExpressions.Regex.Replace(GetType().Name.Replace("Node", ""), "([a-z])([A-Z])", "$1 $2");
        }
    }
}
