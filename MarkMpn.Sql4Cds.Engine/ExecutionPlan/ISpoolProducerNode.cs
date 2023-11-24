using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Provides an interface for nodes that spool data internally
    /// </summary>
    interface ISpoolProducerNode : IDataExecutionPlanNodeInternal
    {
        /// <summary>
        /// Accesses the spooled data
        /// </summary>
        /// <returns>The sequence of data that has been spooled</returns>
        IEnumerable<Entity> GetWorkTable();

        /// <summary>
        /// Returns the last cloned version of this node
        /// </summary>
        ISpoolProducerNode LastClone { get; }
    }
}
