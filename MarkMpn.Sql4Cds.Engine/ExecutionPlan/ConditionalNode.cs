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
    class ConditionalNode : BaseNode, IControlOfFlowNode
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

        [Browsable(false)]
        public ConditionalNodeType Type { get; set; }

        [Category("Conditional")]
        [Description("The condition that must be true for execution to continue")]
        public BooleanExpression Condition { get; set; }

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Browsable(false)]
        public string SourceColumn { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] TrueStatements { get; set; }

        [Browsable(false)]
        public IRootExecutionPlanNodeInternal[] FalseStatements { get; set; }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            if (Source != null)
                Source.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));

            foreach (var node in TrueStatements)
                node.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));

            if (FalseStatements != null)
            {
                foreach (var node in FalseStatements)
                    node.AddRequiredColumns(dataSources, parameterTypes, new List<string>(requiredColumns));
            }
        }

        public IRootExecutionPlanNodeInternal[] Execute(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, out bool rerun)
        {
            using (_timer.Run())
            {
                try
                {
                    _executionCount++;
                    rerun = false;

                    bool result;

                    if (_condition != null)
                    {
                        result = _condition(null, parameterValues, options);
                    }
                    else
                    {
                        var record = Source.Execute(dataSources, options, parameterTypes, parameterValues).First();
                        result = ((SqlInt32)record[SourceColumn]).Value == 1;
                    }

                    if (result)
                    {
                        if (Type == ConditionalNodeType.While)
                            rerun = true;

                        return TrueStatements;
                    }

                    return FalseStatements;
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

        public IRootExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            if (Source != null)
                Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);

            for (var i = 0; i < TrueStatements.Length; i++)
                TrueStatements[i] = TrueStatements[i].FoldQuery(dataSources, options, parameterTypes, hints);

            if (FalseStatements != null)
            {
                for (var i = 0; i < FalseStatements.Length; i++)
                    FalseStatements[i] = FalseStatements[i].FoldQuery(dataSources, options, parameterTypes, hints);
            }

            _condition = Condition?.Compile(null, parameterTypes);

            return this;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (Source != null)
                yield return Source;

            foreach (var stmt in TrueStatements)
                yield return stmt;

            if (FalseStatements != null)
            {
                foreach (var stmt in FalseStatements)
                    yield return stmt;
            }
        }

        public override string ToString()
        {
            var name = base.ToString();

            if (Source != null)
                name += "\r\n(With Query)";

            return name;
        }
    }

    enum ConditionalNodeType
    {
        If,
        While
    }
}
