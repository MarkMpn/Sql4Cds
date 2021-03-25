using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    public class TryCatchNode : BaseDataNode
    {
        public IDataExecutionPlanNode TrySource { get; set; }
        public IDataExecutionPlanNode CatchSource { get; set; }

        [Browsable(false)]
        public Func<Exception,bool> ExceptionFilter { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IOrganizationService org, IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var useCatchSource = false;
            IEnumerator<Entity> enumerator;

            try
            {
                enumerator = TrySource.Execute(org, metadata, options, parameterTypes, parameterValues).GetEnumerator();
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
                foreach (var entity in CatchSource.Execute(org, metadata, options, parameterTypes, parameterValues))
                    yield return entity;
            }
        }

        public override NodeSchema GetSchema(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes)
        {
            return TrySource.GetSchema(metadata, parameterTypes);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return TrySource;
            yield return CatchSource;
        }

        public override IDataExecutionPlanNode FoldQuery(IAttributeMetadataCache metadata, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            TrySource = TrySource.FoldQuery(metadata, options, parameterTypes);
            TrySource.Parent = this;
            CatchSource = CatchSource.FoldQuery(metadata, options, parameterTypes);
            CatchSource.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            TrySource.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
            CatchSource.AddRequiredColumns(metadata, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IAttributeMetadataCache metadata, IDictionary<string, Type> parameterTypes, ITableSizeCache tableSize)
        {
            return TrySource.EstimateRowsOut(metadata, parameterTypes, tableSize);
        }
    }
}
