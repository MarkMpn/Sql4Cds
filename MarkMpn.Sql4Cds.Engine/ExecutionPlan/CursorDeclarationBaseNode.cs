using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    abstract class CursorDeclarationBaseNode : BaseNode, IDmlQueryExecutionPlanNode
    {
        public bool _isOpen;

        public string Sql { get; set; }
        public int Index { get; set; }
        public int Length { get; set; }
        public int LineNumber { get; set; }
        public override int ExecutionCount => 0;

        public override TimeSpan Duration => TimeSpan.Zero;

        public string CursorName { get; set; }

        public CursorOptionKind Scope { get; set; }

        public CursorOptionKind Direction { get; set; }

        public IDmlQueryExecutionPlanNode PopulationQuery { get; set; }

        public IDataReaderExecutionPlanNode FetchQuery { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (PopulationQuery != null)
                PopulationQuery.AddRequiredColumns(context, new List<string>());

            if (FetchQuery != null)
                FetchQuery.AddRequiredColumns(context, new List<string>());
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (PopulationQuery != null)
                yield return PopulationQuery;

            if (FetchQuery != null)
                yield return FetchQuery;
        }

        public abstract object Clone();

        public IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (PopulationQuery != null)
                PopulationQuery = (IDmlQueryExecutionPlanNode)PopulationQuery.FoldQuery(context, hints).Single();

            if (FetchQuery is IDataExecutionPlanNodeInternal fetch)
                FetchQuery = (IDataReaderExecutionPlanNode)fetch.FoldQuery(context, hints);

            return new[] { this };
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            try
            {
                // Make sure a cursor with this name doesn't already exist in the selected scope
                IDictionary<string, CursorDeclarationBaseNode> cursors;

                if (Scope == CursorOptionKind.Global)
                    cursors = context.Session.Cursors;
                else
                    cursors = context.Cursors;

                if (cursors.ContainsKey(CursorName))
                    throw new QueryExecutionException(Sql4CdsError.DuplicateCursorName(CursorName));

                // Store a copy of this cursor in the session so it can be referenced by later FETCH statements
                cursors[CursorName] = (CursorDeclarationBaseNode)Clone();

                // Remove the population and fetch queries from this clone so they doesn't appear in the execution plan
                recordsAffected = -1;
                message = null;

                PopulationQuery = null;
                FetchQuery = null;
            }
            catch (QueryExecutionException ex)
            {
                if (ex.Node == null)
                    ex.Node = this;

                throw;
            }
            catch (Exception ex)
            {
                throw new QueryExecutionException(Sql4CdsError.InternalError(ex.Message), ex) { Node = this };
            }
        }

        public virtual IDmlQueryExecutionPlanNode Open(NodeExecutionContext context)
        {
            if (_isOpen)
                throw new QueryExecutionException(Sql4CdsError.CursorAlreadyOpen());

            _isOpen = true;
            return (IDmlQueryExecutionPlanNode)PopulationQuery.Clone();
        }

        public virtual void Close(NodeExecutionContext context)
        {
            if (!_isOpen)
                throw new QueryExecutionException(Sql4CdsError.CursorNotOpen());

            _isOpen = false;
        }

        public virtual IDataReaderExecutionPlanNode Fetch(NodeExecutionContext context, FetchOrientation orientation, Func<ExpressionExecutionContext,object> rowOffset)
        {
            if (!_isOpen)
                throw new QueryExecutionException(Sql4CdsError.CursorNotOpen());

            if (orientation != FetchOrientation.Next && Direction == CursorOptionKind.ForwardOnly)
                throw new QueryExecutionException(Sql4CdsError.CursorFetchTypeNotSupportedWithForwardOnly(orientation));

            // Reset the @@FETCH_STATUS variable
            context.ParameterValues["@@FETCH_STATUS"] = (SqlInt32)(-1);

            return (IDataReaderExecutionPlanNode)FetchQuery.Clone();
        }
    }
}
