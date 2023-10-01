using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;

namespace MarkMpn.Sql4Cds.Engine
{
    public static class TSqlFragmentExtensions
    {
        /// <summary>
        /// Converts a <see cref="TSqlFragment"/> to the corresponding SQL string
        /// </summary>
        /// <param name="fragment">The SQL DOM fragment to convert</param>
        /// <returns>The SQL string that the fragment can be parsed from</returns>
        public static string ToSql(this TSqlFragment fragment)
        {
            if (fragment.ScriptTokenStream != null)
            {
                return String.Join("",
                    fragment.ScriptTokenStream
                        .Skip(fragment.FirstTokenIndex)
                        .Take(fragment.LastTokenIndex - fragment.FirstTokenIndex + 1)
                        .Select(t => t.Text));
            }

            new Sql160ScriptGenerator().GenerateScript(fragment, out var sql);
            return sql;
        }

        /// <summary>
        /// Creates a clone of a <see cref="TSqlFragment"/>
        /// </summary>
        /// <typeparam name="T">The type of <see cref="TSqlFragment"/> being cloned</typeparam>
        /// <param name="fragment">The fragment to clone</param>
        /// <returns>A clone of the requested <paramref name="fragment"/></returns>
        public static T Clone<T>(this T fragment) where T : TSqlFragment
        {
            if (fragment == null)
                return null;

            if (fragment is ColumnReferenceExpression col)
            {
                return (T)(object)new ColumnReferenceExpression
                {
                    Collation = col.Collation.Clone(),
                    ColumnType = col.ColumnType,
                    MultiPartIdentifier = col.MultiPartIdentifier.Clone(),
                };
            }

            if (fragment is Identifier id)
            {
                return (T)(object)new Identifier
                {
                    QuoteType = id.QuoteType,
                    Value = id.Value,
                };
            }

            if (fragment is IdentifierLiteral guid)
            {
                return (T)(object)new IdentifierLiteral
                {
                    Collation = guid.Collation.Clone(),
                    QuoteType = guid.QuoteType,
                    Value = guid.Value,
                };
            }

            if (fragment is IntegerLiteral i)
            {
                return (T)(object)new IntegerLiteral
                {
                    Collation = i.Collation.Clone(),
                    Value = i.Value,
                };
            }

            if (fragment is MoneyLiteral money)
            {
                return (T)(object)new MoneyLiteral
                {
                    Collation = money.Collation.Clone(),
                    Value = money.Value,
                };
            }

            if (fragment is NullLiteral n)
            {
                return (T)(object)new NullLiteral
                {
                    Collation = n.Collation.Clone(),
                    Value = n.Value,
                };
            }

            if (fragment is NumericLiteral num)
            {
                return (T)(object)new NumericLiteral
                {
                    Collation = num.Collation.Clone(),
                    Value = num.Value,
                };
            }

            if (fragment is RealLiteral real)
            {
                return (T)(object)new RealLiteral
                {
                    Collation = real.Collation.Clone(),
                    Value = real.Value,
                };
            }

            if (fragment is StringLiteral str)
            {
                return (T)(object)new StringLiteral
                {
                    Collation = str.Collation.Clone(),
                    IsLargeObject = str.IsLargeObject,
                    IsNational = str.IsNational,
                    Value = str.Value,
                };
            }

            if (fragment is OdbcLiteral odbc)
            {
                return (T)(object)new OdbcLiteral
                {
                    Collation = odbc.Collation.Clone(),
                    IsNational = odbc.IsNational,
                    OdbcLiteralType = odbc.OdbcLiteralType,
                    Value = odbc.Value,
                };
            }

            if (fragment is MaxLiteral max)
            {
                return (T)(object)new MaxLiteral
                {
                    Collation = max.Collation.Clone(),
                    Value = max.Value
                };
            }

            if (fragment is BooleanBinaryExpression boolBin)
            {
                return (T)(object)new BooleanBinaryExpression
                {
                    BinaryExpressionType = boolBin.BinaryExpressionType,
                    FirstExpression = boolBin.FirstExpression.Clone(),
                    SecondExpression = boolBin.SecondExpression.Clone(),
                };
            }

            if (fragment is BooleanComparisonExpression cmp)
            {
                return (T)(object)new BooleanComparisonExpression
                {
                    ComparisonType = cmp.ComparisonType,
                    FirstExpression = cmp.FirstExpression.Clone(),
                    SecondExpression = cmp.SecondExpression.Clone(),
                };
            }

            if (fragment is BooleanParenthesisExpression boolParen)
            {
                return (T)(object)new BooleanParenthesisExpression
                {
                    Expression = boolParen.Expression.Clone(),
                };
            }

            if (fragment is InPredicate inPred)
            {
                var clone = new InPredicate
                {
                    Expression = inPred.Expression.Clone(),
                    NotDefined = inPred.NotDefined,
                    Subquery = inPred.Subquery.Clone(),
                };

                foreach (var value in inPred.Values)
                    clone.Values.Add(value.Clone());

                return (T)(object)clone;
            }

            if (fragment is BooleanIsNullExpression isNull)
            {
                return (T)(object)new BooleanIsNullExpression
                {
                    Expression = isNull.Expression.Clone(),
                    IsNot = isNull.IsNot,
                };
            }

            if (fragment is LikePredicate like)
            {
                return (T)(object)new LikePredicate
                {
                    EscapeExpression = like.EscapeExpression.Clone(),
                    FirstExpression = like.FirstExpression.Clone(),
                    NotDefined = like.NotDefined,
                    OdbcEscape = like.OdbcEscape,
                    SecondExpression = like.SecondExpression.Clone(),
                };
            }

            if (fragment is BooleanNotExpression not)
            {
                return (T)(object)new BooleanNotExpression
                {
                    Expression = not.Expression.Clone(),
                };
            }

            if (fragment is FullTextPredicate fullText)
            {
                var clone = new FullTextPredicate
                {
                    FullTextFunctionType = fullText.FullTextFunctionType,
                    LanguageTerm = fullText.LanguageTerm.Clone(),
                    PropertyName = fullText.PropertyName.Clone(),
                    Value = fullText.Value.Clone(),
                };

                foreach (var c in fullText.Columns)
                    clone.Columns.Add(c.Clone());

                return (T)(object)clone;
            }

            if (fragment is BinaryExpression bin)
            {
                return (T)(object)new BinaryExpression
                {
                    BinaryExpressionType = bin.BinaryExpressionType,
                    FirstExpression = bin.FirstExpression.Clone(),
                    SecondExpression = bin.SecondExpression.Clone(),
                };
            }

            if (fragment is FunctionCall func)
            {
                var clone = new FunctionCall
                {
                    CallTarget = func.CallTarget.Clone(),
                    Collation = func.Collation.Clone(),
                    FunctionName = func.FunctionName.Clone(),
                    OverClause = func.OverClause.Clone(),
                    UniqueRowFilter = func.UniqueRowFilter,
                    WithinGroupClause = func.WithinGroupClause.Clone(),
                };

                foreach (var p in func.Parameters)
                    clone.Parameters.Add(p.Clone());

                return (T)(object)clone;
            }

            if (fragment is ParenthesisExpression paren)
            {
                return (T)(object)new ParenthesisExpression
                {
                    Collation = paren.Collation.Clone(),
                    Expression = paren.Expression.Clone(),
                };
            }

            if (fragment is UnaryExpression unary)
            {
                return (T)(object)new UnaryExpression
                {
                    Expression = unary.Expression.Clone(),
                    UnaryExpressionType = unary.UnaryExpressionType,
                };
            }

            if (fragment is VariableReference var)
            {
                return (T)(object)new VariableReference
                {
                    Collation = var.Collation.Clone(),
                    Name = var.Name,
                };
            }

            if (fragment is SimpleCaseExpression simpleCase)
            {
                var clone = new SimpleCaseExpression
                {
                    Collation = simpleCase.Collation.Clone(),
                    ElseExpression = simpleCase.ElseExpression.Clone(),
                    InputExpression = simpleCase.InputExpression.Clone(),
                };

                foreach (var w in simpleCase.WhenClauses)
                    clone.WhenClauses.Add(w.Clone());

                return (T)(object)clone;
            }

            if (fragment is SimpleWhenClause simpleWhenClause)
            {
                return (T)(object)new SimpleWhenClause
                {
                    ThenExpression = simpleWhenClause.ThenExpression.Clone(),
                    WhenExpression = simpleWhenClause.WhenExpression.Clone(),
                };
            }

            if (fragment is SearchedCaseExpression searchedCase)
            {
                var clone = new SearchedCaseExpression
                {
                    Collation = searchedCase.Collation.Clone(),
                    ElseExpression = searchedCase.ElseExpression.Clone(),
                };

                foreach (var w in searchedCase.WhenClauses)
                    clone.WhenClauses.Add(w.Clone());

                return (T)(object)clone;
            }

            if (fragment is SearchedWhenClause searchedWhenClause)
            {
                return (T)(object)new SearchedWhenClause
                {
                    ThenExpression = searchedWhenClause.ThenExpression.Clone(),
                    WhenExpression = searchedWhenClause.WhenExpression.Clone(),
                };
            }

            if (fragment is ConvertCall convert)
            {
                return (T)(object)new ConvertCall
                {
                    Collation = convert.Collation.Clone(),
                    DataType = convert.DataType.Clone(),
                    Parameter = convert.Parameter.Clone(),
                    Style = convert.Style.Clone(),
                };
            }

            if (fragment is SqlDataTypeReference sqlDataType)
            {
                var clone = new SqlDataTypeReference
                {
                    Name = sqlDataType.Name.Clone(),
                    SqlDataTypeOption = sqlDataType.SqlDataTypeOption,
                };

                foreach (var p in sqlDataType.Parameters)
                    clone.Parameters.Add(p.Clone());

                return (T)(object)clone;
            }

            if (fragment is SchemaObjectName name)
            {
                var clone = new SchemaObjectName();

                foreach (var nameId in name.Identifiers)
                    clone.Identifiers.Add(nameId.Clone());

                return (T)(object)clone;
            }

            if (fragment is MultiPartIdentifier mid)
            {
                var clone = new MultiPartIdentifier();

                foreach (var cid in mid.Identifiers)
                    clone.Identifiers.Add(cid.Clone());

                return (T)(object)clone;
            }

            if (fragment is UserDataTypeReference userDataType)
            {
                var clone = new UserDataTypeReference
                {
                    Name = userDataType.Name.Clone(),
                };

                foreach (var p in userDataType.Parameters)
                    clone.Parameters.Add(p.Clone());

                return (T)(object)clone;
            }

            if (fragment is XmlDataTypeReference xmlDataType)
            {
                return (T)(object)new XmlDataTypeReference
                {
                    Name = xmlDataType.Name.Clone(),
                    XmlDataTypeOption = xmlDataType.XmlDataTypeOption,
                    XmlSchemaCollection = xmlDataType.XmlSchemaCollection?.Clone()
                };
            }

            if (fragment is CastCall cast)
            {
                return (T)(object)new CastCall
                {
                    Collation = cast.Collation.Clone(),
                    DataType = cast.DataType.Clone(),
                    Parameter = cast.Parameter.Clone(),
                };
            }

            if (fragment is ParameterlessCall parameterless)
            {
                return (T)(object)new ParameterlessCall
                {
                    Collation = parameterless.Collation.Clone(),
                    ParameterlessCallType = parameterless.ParameterlessCallType,
                };
            }

            if (fragment is GlobalVariableExpression global)
            {
                return (T)(object)new GlobalVariableExpression
                {
                    Collation = global.Collation.Clone(),
                    Name = global.Name,
                };
            }

            if (fragment is ExpressionWithSortOrder sort)
            {
                return (T)(object)new ExpressionWithSortOrder
                {
                    Expression = sort.Expression.Clone(),
                    SortOrder = sort.SortOrder,
                };
            }

            if (fragment is ExpressionCallTarget callTarget)
            {
                return (T)(object)new ExpressionCallTarget
                {
                    Expression = callTarget.Expression.Clone(),
                };
            }

            if (fragment is DistinctPredicate distinct)
            {
                return (T)(object)new DistinctPredicate
                {
                    IsNot = distinct.IsNot,
                    FirstExpression = distinct.FirstExpression.Clone(),
                    SecondExpression = distinct.SecondExpression.Clone(),
                };
            }

            if (fragment is QuerySpecification querySpec)
            {
                var clone = new QuerySpecification
                {
                    ForClause = querySpec.ForClause?.Clone(),
                    FromClause = querySpec.FromClause?.Clone(),
                    GroupByClause = querySpec.GroupByClause?.Clone(),
                    HavingClause = querySpec.HavingClause?.Clone(),
                    OffsetClause = querySpec.OffsetClause?.Clone(),
                    OrderByClause = querySpec.OrderByClause?.Clone(),
                    TopRowFilter = querySpec.TopRowFilter?.Clone(),
                    UniqueRowFilter = querySpec.UniqueRowFilter,
                    WhereClause = querySpec.WhereClause?.Clone(),
                    WindowClause = querySpec.WindowClause?.Clone()
                };

                foreach (var selectElement in querySpec.SelectElements)
                    clone.SelectElements.Add(selectElement.Clone());

                return (T)(object)clone;
            }

            if (fragment is FromClause from)
            {
                var clone = new FromClause();

                foreach (var predict in from.PredictTableReference)
                    clone.PredictTableReference.Add(predict.Clone());

                foreach (var table in from.TableReferences)
                    clone.TableReferences.Add(table.Clone());

                return (T)(object)clone;
            }

            if (fragment is NamedTableReference tableRef)
            {
                var clone = new NamedTableReference
                {
                    Alias = tableRef.Alias?.Clone(),
                    ForPath = tableRef.ForPath,
                    SchemaObject = tableRef.SchemaObject.Clone(),
                    TableSampleClause = tableRef.TableSampleClause?.Clone(),
                    TemporalClause = tableRef.TemporalClause?.Clone(),
                };

                foreach (var hint in tableRef.TableHints)
                    clone.TableHints.Add(hint.Clone());

                return (T)(object)clone;
            }

            if (fragment is QualifiedJoin qualifiedJoin)
            {
                return (T)(object)new QualifiedJoin
                {
                    FirstTableReference = qualifiedJoin.FirstTableReference.Clone(),
                    JoinHint = qualifiedJoin.JoinHint,
                    QualifiedJoinType = qualifiedJoin.QualifiedJoinType,
                    SearchCondition = qualifiedJoin.SearchCondition?.Clone(),
                    SecondTableReference = qualifiedJoin.SecondTableReference.Clone()
                };
            }

            if (fragment is GroupByClause groupBy)
            {
                var clone = new GroupByClause
                {
                    All = groupBy.All,
                    GroupByOption = groupBy.GroupByOption
                };

                foreach (var groupBySpec in groupBy.GroupingSpecifications)
                    clone.GroupingSpecifications.Add(groupBySpec.Clone());

                return (T)(object)clone;
            }

            if (fragment is WhereClause where)
            {
                return (T)(object)new WhereClause
                {
                    Cursor = where.Cursor?.Clone(),
                    SearchCondition = where.SearchCondition?.Clone()
                };
            }

            if (fragment is SelectScalarExpression selectScalarExpression)
            {
                return (T)(object)new SelectScalarExpression
                {
                    ColumnName = selectScalarExpression.ColumnName?.Clone(),
                    Expression = selectScalarExpression.Expression?.Clone()
                };
            }

            throw new NotSupportedQueryFragmentException("Unhandled expression type " + fragment.GetType().Name, fragment);
        }
    }
}
