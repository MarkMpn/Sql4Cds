using MarkMpn.Sql4Cds.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Linq;

namespace MarkMpn.Sql4Cds
{
    static class FetchXml2Sql
    {
        public static string Convert(FetchXml.FetchType fetch)
        {
            var select = new SelectStatement();
            var query = new QuerySpecification();
            select.QueryExpression = query;

            if (fetch.top != null)
                query.TopRowFilter = new TopRowFilter { Expression = new IntegerLiteral { Value = fetch.top } };

            if (fetch.distinct)
                query.UniqueRowFilter = UniqueRowFilter.Distinct;

            var entity = fetch.Items.OfType<FetchEntityType>().SingleOrDefault();

            AddSelectElements(query, entity.Items, entity?.name);

            // From
            if (entity != null)
            {
                query.FromClause = new FromClause
                {
                    TableReferences =
                    {
                        new NamedTableReference
                        {
                            SchemaObject = new SchemaObjectName
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = entity.name }
                                }
                            }
                        }
                    }
                };

                query.FromClause.TableReferences[0] = BuildJoins(query.FromClause.TableReferences[0], (NamedTableReference)query.FromClause.TableReferences[0], entity.Items, query);
            }

            // Offset
            if (!String.IsNullOrEmpty(fetch.page) && fetch.page != "1")
            {
                var page = Int32.Parse(fetch.page);
                var pageSize = Int32.Parse(fetch.count);

                query.OffsetClause = new OffsetClause
                {
                    OffsetExpression = new IntegerLiteral { Value = ((page - 1) * pageSize).ToString() },
                    FetchExpression = new IntegerLiteral { Value = fetch.count }
                };
            }

            // Where
            var filter = GetFilter(entity.Items, entity.name);
            if (filter != null)
            {
                query.WhereClause = new WhereClause
                {
                    SearchCondition = filter
                };
            }

            // Order By
            foreach (var sort in entity.Items.OfType<FetchOrderType>())
            {
                if (query.OrderByClause == null)
                    query.OrderByClause = new OrderByClause();

                if (!String.IsNullOrEmpty(sort.alias))
                {
                    query.OrderByClause.OrderByElements.Add(new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier{Value = sort.alias}
                                }
                            }
                        },
                        SortOrder = sort.descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
                else
                {
                    query.OrderByClause.OrderByElements.Add(new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                            {
                                new Identifier{Value = entity.name},
                                new Identifier{Value = sort.attribute}
                            }
                            }
                        },
                        SortOrder = sort.descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
            }

            if (query.FromClause.TableReferences[0] is NamedTableReference)
            {
                select.Accept(new SimplifyMultiPartIdentifierVisitor(entity.name));
            }

            new Sql150ScriptGenerator().GenerateScript(select, out var sql);

            return sql;
        }

        private static void AddSelectElements(QuerySpecification query, object[] items, string prefix)
        {
            foreach (var all in items.OfType<allattributes>())
            {
                query.SelectElements.Add(new SelectStarExpression
                {
                    Qualifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier{Value = prefix}
                        }
                    }
                });
            }

            foreach (var attr in items.OfType<FetchAttributeType>())
            {
                var element = new SelectScalarExpression();
                var col = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier{Value = prefix},
                            new Identifier{Value = attr.name}
                        }
                    }
                };

                if (attr.aggregateSpecified)
                {
                    var func = new FunctionCall
                    {
                        FunctionName = new Identifier { Value = attr.aggregate == AggregateType.countcolumn ? "count" : attr.aggregate.ToString() },
                        Parameters =
                        {
                            col
                        }
                    };
                    element.Expression = func;
                }
                else
                {
                    element.Expression = col;
                }

                if (!String.IsNullOrEmpty(attr.alias) && (attr.aggregateSpecified || attr.alias != attr.name))
                    element.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = attr.alias } };

                query.SelectElements.Add(element);

                if (attr.groupbySpecified && attr.groupby == FetchBoolType.@true)
                {
                    if (query.GroupByClause == null)
                        query.GroupByClause = new GroupByClause();

                    query.GroupByClause.GroupingSpecifications.Add(new ExpressionGroupingSpecification { Expression = col });
                }
            }
        }

        private static TableReference BuildJoins(TableReference dataSource, NamedTableReference parentTable, object[] items, QuerySpecification query)
        {
            foreach (var link in items.OfType<FetchLinkEntityType>())
            {
                var join = new QualifiedJoin
                {
                    FirstTableReference = dataSource,
                    SecondTableReference = new NamedTableReference
                    {
                        SchemaObject = new SchemaObjectName
                        {
                            Identifiers =
                            {
                                new Identifier
                                {
                                    Value = link.name
                                }
                            }
                        },
                        Alias = String.IsNullOrEmpty(link.alias) ? null : new Identifier { Value = link.alias }
                    },
                    QualifiedJoinType = link.linktype == "inner" ? QualifiedJoinType.Inner : QualifiedJoinType.LeftOuter,
                    SearchCondition = new BooleanComparisonExpression
                    {
                        FirstExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = parentTable.Alias?.Value ?? parentTable.SchemaObject.Identifiers.Last().Value },
                                    new Identifier { Value = link.to }
                                }
                            }
                        },
                        ComparisonType = BooleanComparisonType.Equals,
                        SecondExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier{ Value = link.alias ?? link.name },
                                    new Identifier { Value = link.from }
                                }
                            }
                        }
                    }
                };

                dataSource = BuildJoins(join, (NamedTableReference)join.SecondTableReference, link.Items, query);

                // Select
                AddSelectElements(query, link.Items, link.alias ?? link.name);

                // Filter
                var filter = GetFilter(link.Items, link.alias ?? link.name);
                if (filter != null)
                {
                    var finalFilter = new BooleanBinaryExpression
                    {
                        FirstExpression = join.SearchCondition,
                        BinaryExpressionType = BooleanBinaryExpressionType.And,
                        SecondExpression = filter
                    };

                    join.SearchCondition = finalFilter;
                }
            }

            return dataSource;
        }

        private static BooleanExpression GetFilter(object[] items, string prefix)
        {
            var filter = items.OfType<filter>().SingleOrDefault();

            if (filter == null)
                return null;

            return GetFilter(filter, prefix);
        }

        private static BooleanExpression GetFilter(filter filter, string prefix)
        {
            BooleanExpression expression = null;
            var type = filter.type == filterType.and ? BooleanBinaryExpressionType.And : BooleanBinaryExpressionType.Or;

            foreach (var condition in filter.Items.OfType<condition>())
            {
                var newExpression = GetFilter(condition, prefix);

                if (expression == null)
                    expression = newExpression;
                else
                    expression = new BooleanBinaryExpression
                    {
                        FirstExpression = expression,
                        BinaryExpressionType = type,
                        SecondExpression = newExpression
                    };
            }

            foreach (var subFilter in filter.Items.OfType<filter>())
            {
                var newExpression = GetFilter(subFilter, prefix);

                if (expression == null)
                    expression = newExpression;
                else
                    expression = new BooleanBinaryExpression
                    {
                        FirstExpression = expression,
                        BinaryExpressionType = type,
                        SecondExpression = newExpression
                    };
            }

            return expression;
        }

        private static BooleanExpression GetFilter(condition condition, string prefix)
        {
            var field = new ColumnReferenceExpression
            {
                MultiPartIdentifier = new MultiPartIdentifier
                {
                    Identifiers =
                    {
                        new Identifier{Value = condition.entityname ?? prefix},
                        new Identifier{Value = condition.attribute}
                    }
                }
            };

            BooleanComparisonType type;
            ScalarExpression value = new StringLiteral { Value = condition.value };

            switch (condition.@operator)
            {
                case @operator.above:
                case @operator.containvalues:
                case @operator.eqbusinessid:
                case @operator.eqorabove:
                case @operator.eqorunder:
                case @operator.equserid:
                case @operator.equserlanguage:
                case @operator.equseroruserhierarchy:
                case @operator.equseroruserhierarchyandteams:
                case @operator.equseroruserteams:
                case @operator.equserteams:
                case @operator.infiscalperiod:
                case @operator.infiscalperiodandyear:
                case @operator.infiscalyear:
                case @operator.inorafterfiscalperiodandyear:
                case @operator.inorbeforefiscalperiodandyear:
                case @operator.lastfiscalperiod:
                case @operator.lastfiscalyear:
                case @operator.lastmonth:
                case @operator.lastsevendays:
                case @operator.lastweek:
                case @operator.lastxdays:
                case @operator.lastxfiscalperiods:
                case @operator.lastxfiscalyears:
                case @operator.lastxhours:
                case @operator.lastxmonths:
                case @operator.lastxweeks:
                case @operator.lastxyears:
                case @operator.lastyear:
                case @operator.nebusinessid:
                case @operator.neuserid:
                case @operator.nextfiscalperiod:
                case @operator.nextfiscalyear:
                case @operator.nextmonth:
                case @operator.nextsevendays:
                case @operator.nextweek:
                case @operator.nextxdays:
                case @operator.nextxfiscalperiods:
                case @operator.nextxfiscalyears:
                case @operator.nextxhours:
                case @operator.nextxmonths:
                case @operator.nextxweeks:
                case @operator.nextxyears:
                case @operator.nextyear:
                case @operator.notcontainvalues:
                case @operator.notunder:
                case @operator.olderthanxdays:
                case @operator.olderthanxhours:
                case @operator.olderthanxminutes:
                case @operator.olderthanxmonths:
                case @operator.olderthanxweeks:
                case @operator.olderthanxyears:
                case @operator.on:
                case @operator.onorafter:
                case @operator.onorbefore:
                case @operator.thisfiscalperiod:
                case @operator.thisfiscalyear:
                case @operator.thismonth:
                case @operator.thisweek:
                case @operator.thisyear:
                case @operator.today:
                case @operator.tomorrow:
                case @operator.under:
                case @operator.yesterday:
                    type = BooleanComparisonType.Equals;
                    value = new FunctionCall { FunctionName = new Identifier { Value = condition.@operator.ToString() } };

                    if (condition.value != null)
                        ((FunctionCall) value).Parameters.Add(new StringLiteral { Value = condition.value });

                    break;

                case @operator.beginswith:
                case @operator.notbeginwith:
                    return new LikePredicate { FirstExpression = field, SecondExpression = new StringLiteral { Value = condition.value + "%" }, NotDefined = condition.@operator == @operator.notbeginwith };

                case @operator.between:
                case @operator.notbetween:
                    return new BooleanTernaryExpression { FirstExpression = field, TernaryExpressionType = condition.@operator == @operator.between ? BooleanTernaryExpressionType.Between : BooleanTernaryExpressionType.NotBetween, SecondExpression = new StringLiteral { Value = condition.Items[0].Value }, ThirdExpression = new StringLiteral { Value = condition.Items[1].Value } };
                    
                case @operator.endswith:
                case @operator.notendwith:
                    return new LikePredicate { FirstExpression = field, SecondExpression = new StringLiteral { Value = "%" + condition.value }, NotDefined = condition.@operator == @operator.notendwith };

                case @operator.eq:
                    type = BooleanComparisonType.Equals;
                    break;

                case @operator.ge:
                    type = BooleanComparisonType.GreaterThanOrEqualTo;
                    break;

                case @operator.gt:
                    type = BooleanComparisonType.GreaterThan;
                    break;

                case @operator.@in:
                case @operator.notin:
                    var @in = new InPredicate { Expression = field, NotDefined = condition.@operator == @operator.notin };

                    foreach (var val in condition.Items)
                        @in.Values.Add(new StringLiteral{Value = val.Value });

                    return @in;
                    
                case @operator.le:
                    type = BooleanComparisonType.LessThanOrEqualTo;
                    break;

                case @operator.like:
                case @operator.notlike:
                    return new LikePredicate { FirstExpression = field, SecondExpression = new StringLiteral { Value = condition.value }, NotDefined = condition.@operator == @operator.notlike };

                case @operator.lt:
                    type = BooleanComparisonType.LessThan;
                    break;

                case @operator.ne:
                case @operator.neq:
                    type = BooleanComparisonType.NotEqualToBrackets;
                    break;

                case @operator.@null:
                case @operator.notnull:
                    return new BooleanIsNullExpression { Expression = field, IsNot = condition.@operator == @operator.notnull };

                default:
                    throw new NotImplementedException();
            }

            var expression = new BooleanComparisonExpression
            {
                FirstExpression = field,
                ComparisonType = type,
                SecondExpression = value
            };

            return expression;
        }

        private class SimplifyMultiPartIdentifierVisitor : TSqlFragmentVisitor
        {
            private readonly string _target;

            public SimplifyMultiPartIdentifierVisitor(string target)
            {
                _target = target;
            }

            public override void ExplicitVisit(MultiPartIdentifier node)
            {
                if (node.Identifiers.Count == 2 && node.Identifiers[0].Value == _target)
                    node.Identifiers.RemoveAt(0);

                base.ExplicitVisit(node);
            }
        }
    }
}
