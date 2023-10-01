using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Represents a node in an execution plan
    /// </summary>
    /// <remarks>
    /// Ref https://sqlserverfast.com/epr/generic_information/
    /// 
    /// Each node has Init(), GetNext() and Close(). Here this is mapped to
    /// Execute() being Init(), MoveNext() on the enumerator being GetNext()
    /// and Dispose() on the enumerator being Close()
    /// </remarks>
    public interface IExecutionPlanNode
    {
        /// <summary>
        /// Returns the parent of this node
        /// </summary>
        IExecutionPlanNode Parent { get; }

        /// <summary>
        /// Gets the children of this node
        /// </summary>
        /// <returns></returns>
        IEnumerable<IExecutionPlanNode> GetSources();

        /// <summary>
        /// Returns the number of times this node has been executed
        /// </summary>
        int ExecutionCount { get; }

        /// <summary>
        /// Returns the total amount of time spent executing this node, including the time spent calling source nodes
        /// </summary>
        TimeSpan Duration { get; }
    }

    internal interface IExecutionPlanNodeInternal : IExecutionPlanNode, ICloneable
    {
        /// <summary>
        /// Returns or sets the parent of this node
        /// </summary>
        new IExecutionPlanNode Parent { get; set; }

        /// <summary>
        /// Adds columns into the query which are required by preceding nodes
        /// </summary>
        /// <param name="context">The context in which the node is being built</param>
        /// <param name="requiredColumns">The columns which are required by the parent node</param>
        void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns);

        /// <summary>
        /// Notifies the node that all folding of the query is completed
        /// </summary>
        void FinishedFolding();
    }
}
