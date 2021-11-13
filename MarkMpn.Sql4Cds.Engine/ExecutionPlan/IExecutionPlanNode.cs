using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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
        /// Returns or sets the parent of this node
        /// </summary>
        IExecutionPlanNode Parent { get; set; }

        /// <summary>
        /// Gets the children of this node
        /// </summary>
        /// <returns></returns>
        IEnumerable<IExecutionPlanNode> GetSources();

        /// <summary>
        /// Adds 
        /// </summary>
        /// <param name="metadata"></param>
        /// <param name="parameterTypes"></param>
        /// <param name="requiredColumns"></param>
        void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns);

        /// <summary>
        /// Returns the number of times this node has been executed
        /// </summary>
        int ExecutionCount { get; }

        /// <summary>
        /// Returns the total amount of time spent executing this node, including the time spent calling source nodes
        /// </summary>
        TimeSpan Duration { get; }
    }
}
