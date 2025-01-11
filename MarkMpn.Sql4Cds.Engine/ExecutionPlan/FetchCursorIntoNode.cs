using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class FetchCursorIntoNode : BaseCursorNode, IDmlQueryExecutionPlanNode
    {
        private CursorDeclarationBaseNode _cursor;
        private Func<ExpressionExecutionContext, object> _rowOffset;

        public IList<VariableReference> Variables { get; set; }

        public FetchOrientation Orientation { get; set; }

        public ScalarExpression RowOffset { get; set; }

        public override IRootExecutionPlanNodeInternal[] FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (RowOffset != null)
            {
                var ecc = new ExpressionCompilationContext(context, null, null);
                _rowOffset = RowOffset.Compile(ecc);
            }

            return base.FoldQuery(context, hints);
        }

        public void Execute(NodeExecutionContext context, out int recordsAffected, out string message)
        {
            recordsAffected = -1;
            message = null;

            try
            {
                _cursor = GetCursor(context);

                var fetchQuery = _cursor.Fetch(context, Orientation, _rowOffset);

                using (var reader = new FetchStatusDataReader(fetchQuery.Execute(context, CommandBehavior.Default), context))
                {
                    if (reader.Read())
                    {
                        if (reader.FieldCount != Variables.Count)
                            throw new QueryExecutionException(Sql4CdsError.CursorVariableCountMismatch());

                        var eec = new ExpressionExecutionContext(context);

                        for (var i = 0; i < Variables.Count; i++)
                        {
                            var sourceType = reader.GetProviderSpecificFieldType(i).ToSqlType(context.PrimaryDataSource);
                            var targetType = context.ParameterTypes[Variables[i].Name];

                            if (!SqlTypeConverter.CanChangeTypeImplicit(sourceType, targetType))
                                throw new QueryExecutionException(Sql4CdsError.CursorTypeClash(sourceType, targetType));

                            var conversion = SqlTypeConverter.GetConversion(sourceType, targetType);
                            var value = (INullable)reader.GetProviderSpecificValue(i);
                            value = conversion(value, eec);
                            context.ParameterValues[Variables[i].Name] = value;
                        }
                    }
                }
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

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (_cursor?.FetchQuery != null)
                yield return _cursor.FetchQuery;
        }

        public override object Clone()
        {
            return new FetchCursorIntoNode
            {
                CursorName = CursorName,
                Index = Index,
                Length = Length,
                LineNumber = LineNumber,
                Sql = Sql,
                Variables = Variables,
                Orientation = Orientation,
                RowOffset = RowOffset,
                _rowOffset = _rowOffset,
            };
        }
    }
}
