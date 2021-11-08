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
    /// Allows execution of a different query plan if the first one fails
    /// </summary>
    class TryCatchNode : BaseDataNode
    {
        [Browsable(false)]
        public IDataExecutionPlanNode TrySource { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNode CatchSource { get; set; }

        [Browsable(false)]
        public Func<Exception,bool> ExceptionFilter { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var useCatchSource = false;
            IEnumerator<Entity> enumerator;

            try
            {
                enumerator = TrySource.Execute(dataSources, options, parameterTypes, parameterValues).GetEnumerator();
            }
            catch (Exception ex)
            {
                if (ExceptionFilter != null && !ExceptionFilter(ex))
                    throw;

                useCatchSource = true;
                enumerator = null;
            }

            var doneFirst = false;

            while (!useCatchSource && !options.Cancelled)
            {
                Entity current;

                try
                {
                    if (!enumerator.MoveNext())
                        break;

                    doneFirst = true;
                    current = enumerator.Current;
                }
                catch (Exception ex)
                {
                    if (doneFirst || ExceptionFilter != null && !ExceptionFilter(ex))
                        throw;

                    useCatchSource = true;
                    current = null;
                }
                
                if (!useCatchSource)
                    yield return current;
            }

            if (useCatchSource)
            {
                foreach (var entity in CatchSource.Execute(dataSources, options, parameterTypes, parameterValues))
                    yield return entity;
            }
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return TrySource.GetSchema(dataSources, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return TrySource;
            yield return CatchSource;
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            TrySource = TrySource.FoldQuery(dataSources, options, parameterTypes);
            TrySource.Parent = this;
            CatchSource = CatchSource.FoldQuery(dataSources, options, parameterTypes);
            CatchSource.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            TrySource.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
            CatchSource.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return TrySource.EstimateRowsOut(dataSources, options, parameterTypes);
        }
    }
}
