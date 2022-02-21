using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class GoToNode : BaseNode, IGoToNode
    {
        private int _executionCount;
        private readonly Timer _timer = new Timer();
        private Func<Entity, IDictionary<string, object>, IQueryExecutionOptions, bool> _condition;

        public override int ExecutionCount => _executionCount;

        public override TimeSpan Duration => _timer.Duration;

        [Browsable(false)]
        public string Sql { get; set; }

        [Browsable(false)]
        public int Index { get; set; }

        [Browsable(false)]
        public int Length { get; set; }

        [Category("GoTo")]
        public string Label { get; set; }

        [Category("Conditional")]
        [Description("The condition that must be true for execution to continue")]
        public BooleanExpression Condition { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Browsable(false)]
        public string SourceColumn { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            if (Source != null)
                Source.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));
        }

        public object Clone()
        {
            return new GoToNode
            {
                _condition = _condition,
                Sql = Sql,
                Index = Index,
                Length = Length,
                Condition = Condition,
                Source = (IDataExecutionPlanNodeInternal)Source?.Clone(),
                SourceColumn = SourceColumn,
                Label = Label
            };
        }

        public string Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            using (_timer.Run())
            {
                try
                {
                    _executionCount++;

                    bool result;

                    if (_condition != null)
                    {
                        result = _condition(null, parameterValues, options);
                    }
                    else if (Source != null)
                    {
                        var source = (IDataExecutionPlanNodeInternal)Source.Clone();
                        var record = source.Execute(dataSources, options, parameterTypes, parameterValues).First();
                        result = ((SqlInt32)record[SourceColumn]).Value == 1;
                    }
                    else
                    {
                        result = true;
                    }

                    if (result)
                        return Label;

                    return null;
                }
                catch (QueryExecutionException ex)
                {
                    if (ex.Node == null)
                        ex.Node = this;

                    throw;
                }
                catch (Exception ex)
                {
                    throw new QueryExecutionException(ex.Message, ex) { Node = this };
                }
            }
        }

        public IRootExecutionPlanNodeInternal[] FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            if (Source != null)
                Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);

            _condition = Condition?.Compile(null, parameterTypes);

            return new[] { this };
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            return Array.Empty<IExecutionPlanNode>();
        }

        public override string ToString()
        {
            return "GOTO" + ((Source != null || Condition != null) ? " (Conditional)" : "") + "\r\n" + Label;
        }
    }
}
