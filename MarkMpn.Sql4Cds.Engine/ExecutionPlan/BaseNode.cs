using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Metadata.Query;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public abstract class BaseNode : IExecutionPlanNode
    {
        public IExecutionPlanNode Parent { get; set; }

        public abstract int ExecutionCount { get; }

        public abstract TimeSpan Duration { get; }

        public abstract IEnumerable<IDataExecutionPlanNode> GetSources();

        public abstract void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns);

        public override string ToString()
        {
            return System.Text.RegularExpressions.Regex.Replace(GetType().Name.Replace("Node", ""), "([a-z])([A-Z])", "$1 $2");
        }
    }
}
