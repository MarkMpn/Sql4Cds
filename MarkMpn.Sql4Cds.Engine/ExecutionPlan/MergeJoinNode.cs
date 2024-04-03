using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Merges two sorted data sets
    /// </summary>
    class MergeJoinNode : FoldableJoinNode
    {
        [Description("Many to Many")]
        [Category("Merge Join")]
        public bool ManyToMany { get; private set; }

        public MergeJoinNode() { }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            // https://sqlserverfast.com/epr/merge-join/
            // Implemented inner, left outer, right outer and full outer variants
            // Not implemented semi joins
            // TODO: Handle union & concatenate

            // Left & Right: GetNext, mark as unmatched
            var left = LeftSource.Execute(context).GetEnumerator().WithPeekAhead();
            var right = RightSource.Execute(context).GetEnumerator().WithPeekAhead();
            var leftSchema = LeftSource.GetSchema(context);
            var rightSchema = RightSource.GetSchema(context);
            var mergedSchema = GetSchema(context, true);
            var expressionCompilationContext = new ExpressionCompilationContext(context, mergedSchema, null);
            var expressionExecutionContext = new ExpressionExecutionContext(context);
            var additionalJoinCriteria = AdditionalJoinCriteria?.Compile(expressionCompilationContext);

            var hasLeft = left.MoveNext();
            var hasRight = right.MoveNext();
            var leftMatched = false;
            var rightMatched = false;

            var lt = LeftAttribute == null || RightAttribute == null
                ? ConstantResult(false)
                : new BooleanBinaryExpression
                {
                    FirstExpression = new BooleanBinaryExpression
                    {
                        FirstExpression = new BooleanIsNullExpression { Expression = LeftAttribute },
                        BinaryExpressionType = BooleanBinaryExpressionType.And,
                        SecondExpression = new BooleanIsNullExpression { Expression = RightAttribute, IsNot = true }
                    },
                    BinaryExpressionType = BooleanBinaryExpressionType.Or,
                    SecondExpression = new BooleanComparisonExpression
                    {
                        FirstExpression = LeftAttribute,
                        ComparisonType = BooleanComparisonType.LessThan,
                        SecondExpression = RightAttribute
                    }
                }.Compile(expressionCompilationContext);

            var eq = LeftAttribute == null || RightAttribute == null
                ? ConstantResult(true)
                : new BooleanComparisonExpression
                    {
                        FirstExpression = LeftAttribute,
                        ComparisonType = BooleanComparisonType.Equals,
                        SecondExpression = RightAttribute
                    }.Compile(expressionCompilationContext);

            var gt = LeftAttribute == null || RightAttribute == null
                ? ConstantResult(false)
                : new BooleanBinaryExpression
                {
                    FirstExpression = new BooleanBinaryExpression
                    {
                        FirstExpression = new BooleanIsNullExpression { Expression = LeftAttribute, IsNot = true },
                        BinaryExpressionType = BooleanBinaryExpressionType.And,
                        SecondExpression = new BooleanIsNullExpression { Expression = RightAttribute }
                    },
                    BinaryExpressionType = BooleanBinaryExpressionType.Or,
                    SecondExpression = new BooleanComparisonExpression
                    {
                        FirstExpression = LeftAttribute,
                        ComparisonType = BooleanComparisonType.GreaterThan,
                        SecondExpression = RightAttribute
                    }
                }.Compile(expressionCompilationContext);

            string leftAttributeName = null;
            if (LeftAttribute != null)
                leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out leftAttributeName);
            string rightAttributeName = null;
            if (RightAttribute != null)
                rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out rightAttributeName);

            var workTable = new List<Entity>();
            var workTableUnmatched = new HashSet<Entity>();
            var rightSource = right;

            while (!Done(hasLeft, hasRight))
            {
                // Compare key values
                var finalMerged = Merge(hasLeft ? left.Current : null, leftSchema, hasRight ? right.Current : null, rightSchema, false);
                var merged = OutputLeftSchema && OutputRightSchema ? finalMerged : Merge(hasLeft ? left.Current : null, leftSchema, hasRight ? right.Current : null, rightSchema, true);

                expressionExecutionContext.Entity = merged;
                var isLt = lt(expressionExecutionContext);
                var isEq = eq(expressionExecutionContext);
                var isGt = gt(expressionExecutionContext);

                var nextSide = right;

                if (hasLeft && (isLt || !hasRight))
                {
                    nextSide = left;
                }
                else if (isEq)
                {
                    if (ManyToMany && right == rightSource)
                    {
                        // Peek at the next row from the right input and check if the key value is the same as the current right record
                        var sameKeyValue = right.HasPeekAhead && (RightAttribute == null || right.Current[rightAttributeName].Equals(right.PeekAhead[rightAttributeName]));

                        if (sameKeyValue)
                        {
                            // Duplicated rows. Add the current left record to the work table and keep advancing the right input and
                            // adding the rows to the work table until the key changes
                            workTable.Add(right.Current);
                            workTable.Add(right.PeekAhead);

                            while (right.MoveNext() && right.HasPeekAhead && (RightAttribute == null || right.Current[rightAttributeName].Equals(right.PeekAhead[rightAttributeName])))
                                workTable.Add(right.PeekAhead);

                            if (JoinType == QualifiedJoinType.RightOuter || JoinType == QualifiedJoinType.FullOuter)
                                workTableUnmatched = new HashSet<Entity>(workTable);

                            // Use the worktable as the right input for now
                            right = workTable.GetEnumerator().WithPeekAhead();
                            nextSide = right;
                            right.MoveNext();
                        }
                        else
                        {
                            // Unique row on the right input. Keep it as the current row and advance the left input to use it again
                            nextSide = left;
                        }
                    }

                    if ((!leftMatched || !SemiJoin) && (additionalJoinCriteria == null || additionalJoinCriteria(expressionExecutionContext) == true))
                    {
                        yield return finalMerged;
                        leftMatched = true;
                        rightMatched = true;
                        workTableUnmatched.Remove(right.Current);
                    }
                }

                // Next?
                if (nextSide == right)
                {
                    if (!rightMatched && right == rightSource && (JoinType == QualifiedJoinType.RightOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(null, leftSchema, right.Current, rightSchema, false);

                    hasRight = right.MoveNext();
                    rightMatched = false;

                    if (!hasRight && right != rightSource)
                    {
                        // We've reached the end of the work table. Move to the next row from the left input and start back at
                        // the start of the work table.
                        nextSide = left;
                        right = workTable.GetEnumerator().WithPeekAhead();
                        hasRight = right.MoveNext();
                    }
                }

                if (nextSide == left)
                {
                    if (!leftMatched && (JoinType == QualifiedJoinType.LeftOuter || JoinType == QualifiedJoinType.FullOuter))
                        yield return Merge(left.Current, leftSchema, null, rightSchema, false);

                    // If we're using the work table, check if the key is about to change in the left input. If so, discard the 
                    // work table and move back to the right input
                    if (right != rightSource)
                    {
                        var sameKeyValue = left.HasPeekAhead && (LeftAttribute == null || left.Current[leftAttributeName].Equals(left.PeekAhead[leftAttributeName]));

                        if (!sameKeyValue)
                        {
                            right = rightSource;
                            hasRight = right.MoveNext();
                            rightMatched = false;
                            workTable.Clear();

                            if (JoinType == QualifiedJoinType.RightOuter || JoinType == QualifiedJoinType.FullOuter)
                            {
                                foreach (var entity in workTableUnmatched)
                                    yield return Merge(null, leftSchema, entity, rightSchema, false);
                            }
                        }
                    }

                    hasLeft = left.MoveNext();
                    leftMatched = false;
                }
            }
        }

        private Func<ExpressionExecutionContext, bool> ConstantResult(bool result)
        {
            return (_) => result;
        }

        private bool Done(bool hasLeft, bool hasRight)
        {
            if (JoinType == QualifiedJoinType.Inner)
                return !hasLeft || !hasRight;

            if (JoinType == QualifiedJoinType.LeftOuter)
                return !hasLeft;

            if (JoinType == QualifiedJoinType.RightOuter)
                return !hasRight;

            return !hasLeft && !hasRight;
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            var folded = base.FoldQuery(context, hints);

            if (folded != this)
                return folded;

            var hashJoin = new HashJoinNode
            {
                LeftSource = LeftSource,
                RightSource = RightSource,
                LeftAttribute = LeftAttribute,
                RightAttribute = RightAttribute,
                JoinType = JoinType,
                AdditionalJoinCriteria = AdditionalJoinCriteria,
                SemiJoin = SemiJoin
            };

            foreach (var kvp in DefinedValues)
                hashJoin.DefinedValues.Add(kvp);

            hashJoin.LeftSource.Parent = hashJoin;
            hashJoin.RightSource.Parent = hashJoin;

            // Can't use a merge join if the join key types have different sort orders
            if (LeftAttribute != null && RightAttribute != null)
            {
                var leftSchema = LeftSource.GetSchema(context);
                leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftColumn);
                var leftType = leftSchema.Schema[leftColumn].Type;
                
                var rightSchema = RightSource.GetSchema(context);
                rightSchema.ContainsColumn(RightAttribute.GetColumnName(), out var rightColumn);
                var rightType = rightSchema.Schema[rightColumn].Type;

                if (!IsConsistentSortTypes(leftType, rightType))
                    return hashJoin.FoldQuery(context, hints);
            }

            // This is a many-to-many join if the left attribute is not unique
            if (LeftAttribute == null)
            {
                ManyToMany = true;
            }
            else
            {
                var leftSchema = LeftSource.GetSchema(context);
                leftSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var leftColumn);

                ManyToMany = leftSchema.PrimaryKey != leftColumn;
            }

            // If this is a full outer join without an eqijoin predicate we can still use the many-to-many join logic
            // without any join keys.
            if (LeftAttribute == null || RightAttribute == null)
                return this;

            // Can't fold the join down into the FetchXML, so add a sort and try to fold that in instead
            LeftSource = new SortNode
            {
                Source = LeftSource,
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = LeftAttribute,
                        SortOrder = SortOrder.Ascending
                    }
                }
            }.FoldQuery(context, hints);
            LeftSource.Parent = this;

            RightSource = new SortNode
            {
                Source = RightSource,
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = RightAttribute,
                        SortOrder = SortOrder.Ascending
                    }
                }
            }.FoldQuery(context, hints);
            RightSource.Parent = this;

            // If we couldn't fold the sorts, it's probably faster to use a hash join instead if we only want partial results
            var leftSort = LeftSource as SortNode;
            var rightSort = RightSource as SortNode;

            if (leftSort == null && rightSort == null)
                return this;

            hashJoin.LeftSource = leftSort?.Source ?? LeftSource;
            hashJoin.RightSource = rightSort?.Source ?? RightSource;


            var foldedHashJoin = hashJoin.FoldQuery(context, hints);

            if (Parent is TopNode ||
                leftSort != null && rightSort != null)
                return foldedHashJoin;

            LeftSource.Parent = this;
            RightSource.Parent = this;

            return this;
        }

        private bool IsConsistentSortTypes(DataTypeReference leftType, DataTypeReference rightType)
        {
            if (leftType.IsSameAs(rightType))
                return true;

            // Types can be different but have the same logical sort order, e.g. all numeric types
            if (leftType.IsSameAs(DataTypeHelpers.UniqueIdentifier) && rightType.IsSameAs(DataTypeHelpers.EntityReference) ||
                leftType.IsSameAs(DataTypeHelpers.EntityReference) && rightType.IsSameAs(DataTypeHelpers.UniqueIdentifier))
                return true;

            if (leftType is SqlDataTypeReference leftSqlType && rightType is SqlDataTypeReference rightSqlType)
            {
                if (leftSqlType.SqlDataTypeOption.IsNumeric() && rightSqlType.SqlDataTypeOption.IsNumeric())
                    return true;

                if (leftSqlType.SqlDataTypeOption.IsDateTimeType() && rightSqlType.SqlDataTypeOption.IsDateTimeType())
                    return true;

                // Collations need to be the same for string types, but can be different lengths
                if (leftSqlType.SqlDataTypeOption.IsStringType() && rightSqlType.SqlDataTypeOption.IsStringType() &&
                    leftType is SqlDataTypeReferenceWithCollation leftCollation && rightType is SqlDataTypeReferenceWithCollation rightCollation &&
                    leftCollation.Collation.Equals(rightCollation.Collation))
                {
                    return true;
                }
            }

            return false;
        }

        protected override IReadOnlyList<string> GetSortOrder(INodeSchema outerSchema, INodeSchema innerSchema)
        {
            if (LeftAttribute == null || RightAttribute == null)
                return null;

            outerSchema.ContainsColumn(LeftAttribute.GetColumnName(), out var left);
            innerSchema.ContainsColumn(RightAttribute.GetColumnName(), out var right);

            if (JoinType == QualifiedJoinType.Inner || JoinType == QualifiedJoinType.LeftOuter)
                return new[] { left, right };
            else if (JoinType == QualifiedJoinType.RightOuter)
                return new[] { right, left };
            else
                return null;
        }

        public override object Clone()
        {
            var clone = new MergeJoinNode
            {
                AdditionalJoinCriteria = AdditionalJoinCriteria,
                JoinType = JoinType,
                LeftAttribute = LeftAttribute,
                LeftSource = (IDataExecutionPlanNodeInternal)LeftSource.Clone(),
                RightAttribute = RightAttribute,
                RightSource =  (IDataExecutionPlanNodeInternal)RightSource.Clone(),
                SemiJoin = SemiJoin,
                OutputLeftSchema = OutputLeftSchema,
                OutputRightSchema = OutputRightSchema,
                ManyToMany = ManyToMany
            };

            foreach (var kvp in DefinedValues)
                clone.DefinedValues.Add(kvp);

            clone.LeftSource.Parent = clone;
            clone.RightSource.Parent = clone;

            return clone;
        }
    }

    class EnumeratorWithPeekAhead<T> : IEnumerator<T>
    {
        private readonly IEnumerator<T> _enumerator;

        public EnumeratorWithPeekAhead(IEnumerator<T> enumerator)
        {
            _enumerator = enumerator;
        }

        public T Current { get; private set; }

        public T PeekAhead { get; private set; }

        public bool HasPeekAhead { get; private set; }

        object System.Collections.IEnumerator.Current => Current;

        public bool MoveNext()
        {
            if (HasPeekAhead)
                Current = PeekAhead;
            else if (_enumerator.MoveNext())
                Current = _enumerator.Current;
            else
                return false;

            if (_enumerator.MoveNext())
            {
                PeekAhead = _enumerator.Current;
                HasPeekAhead = true;
            }
            else
            {
                PeekAhead = default(T);
                HasPeekAhead = false;
            }

            return true;
        }

        public void Reset()
        {
            _enumerator.Reset();
            HasPeekAhead = false;
            Current = default(T);
            PeekAhead = default(T);
        }

        public void Dispose()
        {
            _enumerator.Dispose();
        }
    }

    static class EnumeratorExtensions
    {
        public static EnumeratorWithPeekAhead<T> WithPeekAhead<T>(this IEnumerator<T> enumerator)
        {
            return new EnumeratorWithPeekAhead<T>(enumerator);
        }
    }
}
