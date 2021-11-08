using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Checks that each row in the results meets an expected condition
    /// </summary>
    class AssertNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        /// <summary>
        /// The data source for the assertion
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        /// <summary>
        /// The function that must be true for each entity in the <see cref="Source"/>
        /// </summary>
        [Browsable(false)]
        public Func<Entity,bool> Assertion { get; set; }

        /// <summary>
        /// The error message that is generated if any record in the <see cref="Source"/> fails to meet the <see cref="Assertion"/>
        /// </summary>
        [Category("Assert")]
        [Description("The error message that is generated if any record in the source fails to meet the assertion")]
        [DisplayName("Error Message")]
        public string ErrorMessage { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string,object> parameterValues)
        {
            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                if (!Assertion(entity))
                    throw new ApplicationException(ErrorMessage);

                yield return entity;
            }
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(dataSources, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }
    }
}
