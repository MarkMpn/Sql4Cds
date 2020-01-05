using MarkMpn.Sql4Cds.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace MarkMpn.Sql4Cds
{
    static class FetchXml2Sql
    {
        public static string Convert(AttributeMetadataCache metadata, FetchXml.FetchType fetch)
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
            var aliasToLogicalName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

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

                query.FromClause.TableReferences[0] = BuildJoins(metadata, query.FromClause.TableReferences[0], (NamedTableReference)query.FromClause.TableReferences[0], entity.Items, query, aliasToLogicalName);
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
            var filter = GetFilter(metadata, entity.Items, entity.name, aliasToLogicalName);
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

            select.Accept(new QuoteIdentifiersVisitor());

            new Sql150ScriptGenerator().GenerateScript(select, out var sql);

            return sql;
        }

        private static void AddSelectElements(QuerySpecification query, object[] items, string prefix)
        {
            if (items == null)
                return;

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

                    if (attr.distinctSpecified && attr.distinct == FetchBoolType.@true)
                        func.UniqueRowFilter = UniqueRowFilter.Distinct;

                    element.Expression = func;
                }
                else if (attr.dategroupingSpecified)
                {
                    var func = new FunctionCall
                    {
                        FunctionName = new Identifier { Value = "DATEPART" },
                        Parameters =
                        {
                            new ColumnReferenceExpression
                            {
                                MultiPartIdentifier = new MultiPartIdentifier
                                {
                                    Identifiers =
                                    {
                                        new Identifier
                                        {
                                            Value = attr.dategrouping.ToString()
                                        }
                                    }
                                }
                            },
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

                    if (attr.dategroupingSpecified)
                    {
                        query.GroupByClause.GroupingSpecifications.Add(new ExpressionGroupingSpecification
                        {
                            Expression = new FunctionCall
                            {
                                FunctionName = new Identifier { Value = "DATEPART" },
                                Parameters =
                                {
                                    new ColumnReferenceExpression
                                    {
                                        MultiPartIdentifier = new MultiPartIdentifier
                                        {
                                            Identifiers =
                                            {
                                                new Identifier
                                                {
                                                    Value = attr.dategrouping.ToString()
                                                }
                                            }
                                        }
                                    },
                                    col
                                }
                            }
                        });
                    }
                    else
                    {
                        query.GroupByClause.GroupingSpecifications.Add(new ExpressionGroupingSpecification { Expression = col });
                    }
                }
            }
        }

        private static TableReference BuildJoins(AttributeMetadataCache metadata, TableReference dataSource, NamedTableReference parentTable, object[] items, QuerySpecification query, IDictionary<string, string> aliasToLogicalName)
        {
            if (items == null)
                return dataSource;

            foreach (var link in items.OfType<FetchLinkEntityType>())
            {
                if (!String.IsNullOrEmpty(link.alias))
                    aliasToLogicalName[link.alias] = link.name;

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
                    QualifiedJoinType = link.linktype == "outer" ? QualifiedJoinType.LeftOuter : QualifiedJoinType.Inner,
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

                dataSource = BuildJoins(metadata, join, (NamedTableReference)join.SecondTableReference, link.Items, query, aliasToLogicalName);

                // Select
                AddSelectElements(query, link.Items, link.alias ?? link.name);

                // Filter
                var filter = GetFilter(metadata, link.Items, link.alias ?? link.name, aliasToLogicalName);
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

        private static BooleanExpression GetFilter(AttributeMetadataCache metadata, object[] items, string prefix, IDictionary<string, string> aliasToLogicalName)
        {
            if (items == null)
                return null;

            var filter = items.OfType<filter>().SingleOrDefault();

            if (filter == null)
                return null;

            return GetFilter(metadata, filter, prefix, aliasToLogicalName);
        }

        private static BooleanExpression GetFilter(AttributeMetadataCache metadata, filter filter, string prefix, IDictionary<string, string> aliasToLogicalName)
        {
            BooleanExpression expression = null;
            var type = filter.type == filterType.and ? BooleanBinaryExpressionType.And : BooleanBinaryExpressionType.Or;

            foreach (var condition in filter.Items.OfType<condition>())
            {
                var newExpression = GetFilter(metadata, condition, prefix, aliasToLogicalName);

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
                var newExpression = GetFilter(metadata, subFilter, prefix, aliasToLogicalName);

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

        private static BooleanExpression GetFilter(AttributeMetadataCache metadata, condition condition, string prefix, IDictionary<string,string> aliasToLogicalName)
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
            ScalarExpression value;

            if (!aliasToLogicalName.TryGetValue(condition.entityname ?? prefix, out var logicalName))
                logicalName = condition.entityname ?? prefix;

            var meta = metadata[logicalName];
            var attr = meta.Attributes.SingleOrDefault(a => a.LogicalName == condition.attribute);

            if (attr == null)
                value = new StringLiteral { Value = condition.value };
            else if (attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.BigInt ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Integer ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Picklist ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.State ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Status)
                value = new IntegerLiteral { Value = condition.value };
            else if (attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Boolean)
                value = new BinaryLiteral { Value = condition.value };
            else if (attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Decimal ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Double)
                value = new NumericLiteral { Value = condition.value };
            else if (attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Money)
                value = new MoneyLiteral { Value = condition.value };
            else if (attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Lookup ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Owner ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Customer)
                value = new IdentifierLiteral { Value = condition.value };
            else
                value = new StringLiteral { Value = condition.value };

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

        private class QuoteIdentifiersVisitor : TSqlFragmentVisitor
        {
            private static readonly Regex LegalIdentifier = new Regex(@"^[\p{L}_@#][\p{L}\p{Nd}@$#_]*$", RegexOptions.Compiled);
            private static readonly string[] ReservedWords = Properties.Resources.MSSQLReservedWords.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries);

            public override void ExplicitVisit(Identifier node)
            {
                node.QuoteType = RequiresQuote(node.Value) ? QuoteType.SquareBracket : QuoteType.NotQuoted;
                base.ExplicitVisit(node);
            }

            private static bool RequiresQuote(string identifier)
            {
                // Ref. https://msdn.microsoft.com/en-us/library/ms175874.aspx
                var permittedUnquoted = LegalIdentifier.IsMatch(identifier) && Array.BinarySearch(ReservedWords, identifier, StringComparer.OrdinalIgnoreCase) < 0;

                return !permittedUnquoted;
            }
        }
    }
}
