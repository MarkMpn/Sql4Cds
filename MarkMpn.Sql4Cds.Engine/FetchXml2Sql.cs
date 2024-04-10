﻿using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml.Serialization;

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Converts FetchXML to SQL
    /// </summary>
    public static class FetchXml2Sql
    {
        /// <summary>
        /// Converts a FetchXML query to SQL
        /// </summary>
        /// <param name="org">A connection to the CDS organization to retrieve any additional required data from</param>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="fetch">The FetchXML string to convert</param>
        /// <returns>The converted SQL query</returns>
        public static string Convert(IOrganizationService org, IAttributeMetadataCache metadata, string fetch, FetchXml2SqlOptions options, out IDictionary<string,object> parameterValues)
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(fetch)))
            {
                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
                var parsed = (FetchXml.FetchType)serializer.Deserialize(stream);

                return Convert(org, metadata, parsed, options, out parameterValues);
            }
        }

        /// <summary>
        /// Converts a FetchXML query to SQL
        /// </summary>
        /// <param name="org">A connection to the CDS organization to retrieve any additional required data from</param>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="fetch">The query object to convert</param>
        /// <returns>The converted SQL query</returns>
        public static string Convert(IOrganizationService org, IAttributeMetadataCache metadata, FetchXml.FetchType fetch, FetchXml2SqlOptions options, out IDictionary<string,object> parameterValues)
        {
            var ctes = new Dictionary<string, CommonTableExpression>();
            parameterValues = new Dictionary<string, object>();
            var batch = new TSqlBatch();
            var select = new SelectStatement();
            var query = new QuerySpecification();
            select.QueryExpression = query;
            var requiresTimeZone = false;
            var usesToday = false;

            if (fetch.top != null)
                query.TopRowFilter = new TopRowFilter { Expression = new IntegerLiteral { Value = fetch.top } };

            if (fetch.distinct)
                query.UniqueRowFilter = UniqueRowFilter.Distinct;

            // SELECT (columns from first table)
            var entity = fetch.Items.OfType<FetchEntityType>().SingleOrDefault();
            AddSelectElements(query, entity.Items, entity?.name);

            if (query.SelectElements.Count == 0)
                query.SelectElements.Add(new SelectStarExpression());

            // FROM
            var aliasToLogicalName = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            // Link entities can also affect the WHERE clause
            var filter = GetFilter(org, metadata, entity.Items, entity.name, aliasToLogicalName, options, ctes, parameterValues, ref requiresTimeZone, ref usesToday);

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

                if (fetch.DataSource == "archive" || fetch.DataSource == "retained")
                    ((NamedTableReference)query.FromClause.TableReferences[0]).SchemaObject.Identifiers.Insert(0, new Identifier { Value = "archive" });

                if (fetch.nolock)
                    ((NamedTableReference)query.FromClause.TableReferences[0]).TableHints.Add(new TableHint { HintKind = TableHintKind.NoLock });

                // Recurse into link-entities to build joins
                query.FromClause.TableReferences[0] = BuildJoins(org, metadata, query.FromClause.TableReferences[0], (NamedTableReference)query.FromClause.TableReferences[0], entity.Items, query, aliasToLogicalName, fetch.DataSource == "archive", fetch.nolock, options, ctes, parameterValues, ref requiresTimeZone, ref usesToday, ref filter);
            }

            // OFFSET
            if (!String.IsNullOrEmpty(fetch.page) && fetch.page != "1")
            {
                var page = Int32.Parse(fetch.page, CultureInfo.InvariantCulture);
                var pageSize = Int32.Parse(fetch.count, CultureInfo.InvariantCulture);

                query.OffsetClause = new OffsetClause
                {
                    OffsetExpression = new IntegerLiteral { Value = ((page - 1) * pageSize).ToString() },
                    FetchExpression = new IntegerLiteral { Value = fetch.count }
                };
            }

            // WHERE
            if (filter != null)
            {
                query.WhereClause = new WhereClause
                {
                    SearchCondition = filter
                };
            }

            // ORDER BY
            AddOrderBy(entity.name, entity.Items, query, fetch.UseRawOrderBy, metadata, aliasToLogicalName);

            // For single-table queries, don't bother qualifying the column names to make the query easier to read
            if (query.FromClause.TableReferences[0] is NamedTableReference)
                select.Accept(new SimplifyMultiPartIdentifierVisitor(entity.name));

            // Add in any CTEs that were required by the joins or filters
            if (ctes.Count > 0)
            {
                select.WithCtesAndXmlNamespaces = new WithCtesAndXmlNamespaces();

                foreach (var cte in ctes.Values)
                    select.WithCtesAndXmlNamespaces.CommonTableExpressions.Add(cte);
            }

            // Check whether each identifier needs to be quoted so we have minimal quoting to make the query easier to read
            select.Accept(new QuoteIdentifiersVisitor());

            if (usesToday)
            {
                requiresTimeZone = true;

                // Calculate the parameter to work out the calling user's "Today", optionally in UTC
                // FLOOR(CONVERT(float, DATEADD(minute, -@time_zone, GETUTCDATE())))

                // DATEADD(minute, @time_zone, GETUTCDATE())
                var utcNow = new FunctionCall
                {
                    FunctionName = new Identifier { Value = "GETUTCDATE" }
                };

                var localNow = new FunctionCall
                {
                    FunctionName = new Identifier { Value = "DATEADD" },
                    Parameters =
                    {
                        new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "minute" } } } },
                        new VariableReference { Name = "@time_zone" },
                        utcNow
                    }
                };

                var localToday = new FunctionCall
                {
                    FunctionName = new Identifier { Value = "FLOOR" },
                    Parameters =
                    {
                        new ConvertCall
                        {
                            DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.Float },
                            Parameter = localNow
                        }
                    }
                };

                var utcToday = new FunctionCall
                {
                    FunctionName = new Identifier { Value = "DATEADD" },
                    Parameters =
                    {
                        new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier {  Value = "minute"} } } },
                        new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = new VariableReference { Name = "@time_zone" } },
                        new VariableReference { Name = "@today" }
                    }
                };

                var todayParam = new DeclareVariableStatement
                {
                    Declarations =
                    {
                        new DeclareVariableElement
                        {
                            VariableName = new Identifier { Value = "@now" },
                            DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime },
                            Value = localNow
                        },
                        new DeclareVariableElement
                        {
                            VariableName = new Identifier { Value = "@today" },
                            DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime },
                            Value = localToday
                        }
                    }
                };

                batch.Statements.Add(todayParam);

                if (options.ConvertDateTimeToUtc)
                {
                    todayParam.Declarations.Add(new DeclareVariableElement
                    {
                        VariableName = new Identifier { Value = "@utc_now" },
                        DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime },
                        Value = utcNow
                    });

                    // Declare @utc_today variable in different statement as it refers to @today variable which needs to be defined first
                    batch.Statements.Add(new DeclareVariableStatement
                    {
                        Declarations =
                        {
                            new DeclareVariableElement
                            {
                                VariableName = new Identifier { Value = "@utc_today" },
                                DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime },
                                Value = utcToday
                            }
                        }
                    });
                }
            }

            if (requiresTimeZone)
                parameterValues["@time_zone"] = (int) (DateTime.Now - DateTime.UtcNow).TotalMinutes;

            batch.Statements.Add(select);

            new Sql160ScriptGenerator().GenerateScript(batch, out var sql);

            return sql;
        }

        /// <summary>
        /// Adds attributes to the SELECT clause
        /// </summary>
        /// <param name="query">The SQL query to append to the SELECT clause of</param>
        /// <param name="items">The FetchXML items to process</param>
        /// <param name="prefix">The name or alias of the table being processed</param>
        private static void AddSelectElements(QuerySpecification query, object[] items, string prefix)
        {
            if (items == null)
                return;

            // Handle <all-attributes /> as SELECT table.*
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

            // Handle attributes with each combination of aliases, aggregates, distincts, groupings
            // <attribute name="attr" alias="a" aggregate="count" distinct="true" />
            // <attribute name="attr" alias="a" groupby="true" dategrouping="month" />
            foreach (var attr in items.OfType<FetchAttributeType>())
            {
                var element = new SelectScalarExpression();

                // Core column reference
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

                // Apply aggregates or date grouping as function calls
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

                // Apply alias
                if (!String.IsNullOrEmpty(attr.alias) && (attr.aggregateSpecified || attr.alias != attr.name))
                    element.ColumnName = new IdentifierOrValueExpression { Identifier = new Identifier { Value = attr.alias } };

                query.SelectElements.Add(element);

                // Apply grouping
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

        /// <summary>
        /// Recurse through link-entities to add joins to FROM clause and update SELECT clause
        /// </summary>
        /// <param name="org">A connection to the CDS organization to retrieve any additional required data from</param>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="dataSource">The current data source of the SQL query</param>
        /// <param name="parentTable">The details of the table that this new table is being linked to</param>
        /// <param name="items">The FetchXML items in this entity</param>
        /// <param name="query">The current state of the SQL query being built</param>
        /// <param name="aliasToLogicalName">A mapping of table aliases to the logical name</param>
        /// <param name="nolock">Indicates if the NOLOCK table hint should be applied</param>
        /// <returns>The data source including any required joins</returns>
        private static TableReference BuildJoins(IOrganizationService org, IAttributeMetadataCache metadata, TableReference dataSource, NamedTableReference parentTable, object[] items, QuerySpecification query, IDictionary<string, string> aliasToLogicalName, bool archive, bool nolock, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes, IDictionary<string, object> parameters, ref bool requiresTimeZone, ref bool usesToday, ref BooleanExpression where)
        {
            if (items == null)
                return dataSource;

            // Find any <link-entity> elements to process
            foreach (var link in items.OfType<FetchLinkEntityType>())
            {
                // Create the new table reference
                var table = new NamedTableReference
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
                };

                if (archive)
                    table.SchemaObject.Identifiers.Insert(0, new Identifier { Value = "archive" });

                if (nolock)
                    table.TableHints.Add(new TableHint { HintKind = TableHintKind.NoLock });

                if (link.linktype == "exists" || link.linktype == "in" || link.linktype == "matchfirstrowusingcrossapply"
                    || link.linktype == "any" || link.linktype == "not any" || link.linktype == "not all" || link.linktype == "all")
                {
                    // Build a whole new query for the EXISTS subquery
                    var subqueryFilter = GetFilter(org, metadata, link.Items, link.alias ?? link.name, aliasToLogicalName, options, ctes, parameters, ref requiresTimeZone, ref usesToday);

                    var subquery = new QuerySpecification();

                    if (link.linktype == "exists" || link.linktype == "in" || link.linktype == "any" || link.linktype == "not any"
                        || link.linktype == "not all" || link.linktype == "all")
                    {
                        subquery.SelectElements.Add(new SelectScalarExpression
                        {
                            Expression = new ColumnReferenceExpression
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
                        });
                    }
                    else if (link.linktype == "matchfirstrowusingcrossapply")
                    {
                        AddSelectElements(subquery, link.Items, link.alias ?? link.name);
                        AddSelectElements(query, link.Items, link.alias ?? link.name);
                    }

                    subquery.FromClause = new FromClause
                    {
                        TableReferences = { table }
                    };

                    if (archive)
                        ((NamedTableReference)query.FromClause.TableReferences[0]).SchemaObject.Identifiers.Insert(0, new Identifier { Value = "archive" });

                    if (nolock)
                        ((NamedTableReference)query.FromClause.TableReferences[0]).TableHints.Add(new TableHint { HintKind = TableHintKind.NoLock });

                    // Recurse into link-entities to build joins
                    subquery.FromClause.TableReferences[0] = BuildJoins(org, metadata, subquery.FromClause.TableReferences[0], (NamedTableReference)subquery.FromClause.TableReferences[0], link.Items, subquery, aliasToLogicalName, archive, nolock, options, ctes, parameters, ref requiresTimeZone, ref usesToday, ref subqueryFilter);

                    var correlatedFilter = new BooleanComparisonExpression
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
                    };

                    if (link.linktype == "exists" || link.linktype == "matchfirstrowusingcrossapply" || link.linktype == "any"||
                        link.linktype == "not any" || link.linktype == "not all" || link.linktype == "all")
                    {
                        subqueryFilter = CombineExpressions(subqueryFilter, BooleanBinaryExpressionType.And, correlatedFilter);
                    }

                    subquery.WhereClause = new WhereClause { SearchCondition = subqueryFilter };

                    if (link.linktype == "exists" || link.linktype == "any" || link.linktype == "not any" || link.linktype == "not all" || link.linktype == "all")
                    {
                        var existsPredicate = (BooleanExpression) new ExistsPredicate
                        {
                            Subquery = new ScalarSubquery
                            {
                                QueryExpression = subquery
                            }
                        };

                        if (link.linktype == "not any")
                        {
                            existsPredicate = new BooleanNotExpression { Expression = existsPredicate };
                        }
                        else if (link.linktype == "all")
                        {
                            existsPredicate = new BooleanNotExpression { Expression = existsPredicate };
                            var unfilteredQuery = new QuerySpecification
                            {
                                SelectElements = { subquery.SelectElements[0] },
                                FromClause = subquery.FromClause,
                                WhereClause = new WhereClause { SearchCondition = correlatedFilter }
                            };
                            existsPredicate = CombineExpressions(new ExistsPredicate { Subquery = new ScalarSubquery { QueryExpression = unfilteredQuery } }, BooleanBinaryExpressionType.And, existsPredicate);
                        }

                        where = CombineExpressions(where, BooleanBinaryExpressionType.And, existsPredicate);
                    }
                    else if (link.linktype == "in")
                    {
                        var inPredicate = new InPredicate
                        {
                            Expression = new ColumnReferenceExpression
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
                            Subquery = new ScalarSubquery
                            {
                                QueryExpression = subquery
                            }
                        };

                        where = CombineExpressions(where, BooleanBinaryExpressionType.And, inPredicate);
                    }
                    else if (link.linktype == "matchfirstrowusingcrossapply")
                    {
                        subquery.TopRowFilter = new TopRowFilter { Expression = new IntegerLiteral { Value = "1" } };
                        dataSource = new UnqualifiedJoin
                        {
                            FirstTableReference = dataSource,
                            UnqualifiedJoinType = UnqualifiedJoinType.CrossApply,
                            SecondTableReference = new QueryDerivedTable
                            {
                                QueryExpression = subquery,
                                Alias = new Identifier { Value = link.alias ?? link.name }
                            }
                        };
                    }
                    continue;
                }

                // Store the alias of this link
                if (!String.IsNullOrEmpty(link.alias))
                    aliasToLogicalName[link.alias] = link.name;

                // Add the join from the current data source to the new table
                var join = new QualifiedJoin
                {
                    FirstTableReference = dataSource,
                    SecondTableReference = table,
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

                // Update the SELECT clause
                AddSelectElements(query, link.Items, link.alias ?? link.name);

                // Handle any filters within the <link-entity> as additional join criteria
                var filter = GetFilter(org, metadata, link.Items, link.alias ?? link.name, aliasToLogicalName, options, ctes, parameters, ref requiresTimeZone, ref usesToday);

                if (filter != null)
                    join.SearchCondition = CombineExpressions(join.SearchCondition, BooleanBinaryExpressionType.And, filter);

                // Recurse into any other links
                dataSource = BuildJoins(org, metadata, join, (NamedTableReference)join.SecondTableReference, link.Items, query, aliasToLogicalName, archive, nolock, options, ctes, parameters, ref requiresTimeZone, ref usesToday, ref where);
            }

            return dataSource;
        }

        private static BooleanExpression CombineExpressions(BooleanExpression expr1, BooleanBinaryExpressionType type, BooleanExpression expr2)
        {
            if (expr2 is BooleanBinaryExpression bbe && bbe.BinaryExpressionType != type)
                expr2 = new BooleanParenthesisExpression { Expression = expr2 };

            if (expr1 == null)
                return expr2;
            
            if (expr1 is BooleanBinaryExpression lhs && lhs.BinaryExpressionType != type)
                expr2 = new BooleanParenthesisExpression { Expression = expr1 };

            return new BooleanBinaryExpression
            {
                FirstExpression = expr1,
                BinaryExpressionType = type,
                SecondExpression = expr2
            };
        }

        /// <summary>
        /// Converts a FetchXML &lt;filter&gt; to a SQL condition
        /// </summary>
        /// <param name="org">A connection to the CDS organization to retrieve any additional required data from</param>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="items">The items in the &lt;entity&gt; or &lt;link-entity&gt; to process the &lt;filter&gt; from</param>
        /// <param name="prefix">The alias or name of the table that the &lt;filter&gt; applies to</param>
        /// <param name="aliasToLogicalName">The mapping of table alias to logical name</param>
        /// <returns>The SQL condition equivalent of the &lt;filter&gt; found in the <paramref name="items"/>, or <c>null</c> if no filter was found</returns>
        private static BooleanExpression GetFilter(IOrganizationService org, IAttributeMetadataCache metadata, object[] items, string prefix, IDictionary<string, string> aliasToLogicalName, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes, IDictionary<string, object> parameters, ref bool requiresTimeZone, ref bool usesToday)
        {
            if (items == null)
                return null;

            var filters = items.OfType<filter>().ToList();

            BooleanExpression expression = null;

            foreach (var filter in filters)
            {
                var newExpression = GetFilter(org, metadata, filter, prefix, aliasToLogicalName, options, ctes, parameters, ref requiresTimeZone, ref usesToday);

                if (newExpression is BooleanBinaryExpression bbe && bbe.BinaryExpressionType != BooleanBinaryExpressionType.And)
                    newExpression = new BooleanParenthesisExpression { Expression = newExpression };

                if (expression == null)
                {
                    expression = newExpression;
                }
                else
                {
                    if (expression is BooleanBinaryExpression lhs && lhs.BinaryExpressionType != BooleanBinaryExpressionType.And)
                        expression = new BooleanParenthesisExpression { Expression = expression };

                    expression = new BooleanBinaryExpression
                    {
                        FirstExpression = expression,
                        BinaryExpressionType = BooleanBinaryExpressionType.And,
                        SecondExpression = newExpression
                    };
                }
            }

            return expression;
        }

        /// <summary>
        /// Converts a FetchXML &lt;filter&gt; to a SQL condition
        /// </summary>
        /// <param name="org">A connection to the CDS organization to retrieve any additional required data from</param>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="filter">The FetchXML filter to convert</param>
        /// <param name="prefix">The alias or name of the table that the <paramref name="filter"/> applies to</param>
        /// <param name="aliasToLogicalName">The mapping of table alias to logical name</param>
        /// <returns>The SQL condition equivalent of the <paramref name="filter"/></returns>
        private static BooleanExpression GetFilter(IOrganizationService org, IAttributeMetadataCache metadata, filter filter, string prefix, IDictionary<string, string> aliasToLogicalName, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes, IDictionary<string, object> parameters, ref bool requiresTimeZone, ref bool usesToday)
        {
            if (filter.Items == null)
                return null;

            BooleanExpression expression = null;
            var type = filter.type == filterType.and ? BooleanBinaryExpressionType.And : BooleanBinaryExpressionType.Or;

            foreach (var item in filter.Items)
            {
                if (item is condition condition)
                {
                    // Convert each <condition> within the filter
                    var newExpression = GetCondition(org, metadata, condition, prefix, aliasToLogicalName, options, ctes, parameters, ref requiresTimeZone, ref usesToday);

                    expression = CombineExpressions(expression, type, newExpression);
                }
                else if (item is filter subFilter)
                {
                    // Recurse into sub-<filter>s
                    var newExpression = GetFilter(org, metadata, subFilter, prefix, aliasToLogicalName, options, ctes, parameters, ref requiresTimeZone, ref usesToday);

                    expression = CombineExpressions(expression, type, newExpression);
                }
                else if (item is FetchLinkEntityType linkEntity)
                {
                    // Convert related record filters in <link-entity>
                    BooleanExpression newExpression = null;
                    BuildJoins(org, metadata, null, new NamedTableReference { Alias = new Identifier { Value = prefix } }, new[] { item }, null, aliasToLogicalName, false, false, options, ctes, parameters, ref requiresTimeZone, ref usesToday, ref newExpression);

                    expression = CombineExpressions(expression, type, newExpression);
                }
            }

            return expression;
        }

        /// <summary>
        /// Converts a FetchXML &lt;condition&gt; to a SQL condition
        /// </summary>
        /// <param name="org">A connection to the CDS organization to retrieve any additional required data from</param>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="condition">The FetchXML condition to convert</param>
        /// <param name="prefix">The alias or name of the table that the <paramref name="condition"/> applies to</param>
        /// <param name="aliasToLogicalName">The mapping of table alias to logical name</param>
        /// <returns>The SQL condition equivalent of the <paramref name="condition"/></returns>
        private static BooleanExpression GetCondition(IOrganizationService org, IAttributeMetadataCache metadata, condition condition, string prefix, IDictionary<string,string> aliasToLogicalName, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes, IDictionary<string, object> parameters, ref bool requiresTimeZone, ref bool usesToday)
        {
            // Start with the field reference
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

            // Get the metadata for the attribute
            BooleanComparisonType type;
            ScalarExpression value;

            if (!aliasToLogicalName.TryGetValue(condition.entityname ?? prefix, out var logicalName))
                logicalName = condition.entityname ?? prefix;

            var meta = metadata == null ? null : metadata[logicalName];
            var attr = meta == null ? null : meta.Attributes.SingleOrDefault(a => a.LogicalName == condition.attribute);

            var useUtc = false;

            if (options.ConvertDateTimeToUtc && attr is DateTimeAttributeMetadata dtAttr && dtAttr.DateTimeBehavior.Value != DateTimeBehavior.TimeZoneIndependent)
                useUtc = true;

            // Get the literal value to compare to
            object parameterValue = null;
            if (!String.IsNullOrEmpty(condition.ValueOf))
            {
                var parts = condition.ValueOf.Split('.');

                value = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers =
                        {
                            new Identifier{Value = parts.Length == 2 ? parts[0] : condition.entityname ?? prefix},
                            new Identifier{Value = parts.Length == 2 ? parts[1] : condition.ValueOf}
                        }
                    }
                };
            }
            else if (condition.value == null)
            {
                value = new NullLiteral();
            }
            else if (attr == null)
            {
                value = new StringLiteral { Value = condition.value };
                parameterValue = condition.value;
            }
            else if (attr.AttributeType == AttributeTypeCode.BigInt ||
                attr.AttributeType == AttributeTypeCode.Integer ||
                attr.AttributeType == AttributeTypeCode.Picklist ||
                attr.AttributeType == AttributeTypeCode.State ||
                attr.AttributeType == AttributeTypeCode.Status)
            {
                value = new IntegerLiteral { Value = condition.value };
                parameterValue = Int32.Parse(condition.value, CultureInfo.InvariantCulture);
            }
            else if (attr.AttributeType == AttributeTypeCode.Boolean)
            {
                value = new BinaryLiteral { Value = condition.value };
                parameterValue = condition.value == "1";
            }
            else if (attr.AttributeType == AttributeTypeCode.Decimal ||
                attr.AttributeType == AttributeTypeCode.Double)
            {
                value = new NumericLiteral { Value = condition.value };
                parameterValue = Decimal.Parse(condition.value, CultureInfo.InvariantCulture);
            }
            else if (attr.AttributeType == AttributeTypeCode.Money)
            {
                value = new MoneyLiteral { Value = condition.value };
                parameterValue = Decimal.Parse(condition.value, CultureInfo.InvariantCulture);
            }
            else if (attr.AttributeType == AttributeTypeCode.DateTime)
            {
                if (condition.@operator.ToString().Contains("x") && Int32.TryParse(condition.value, out var i))
                {
                    value = new IntegerLiteral { Value = condition.value };
                    parameterValue = i;
                }
                else
                {
                    value = new StringLiteral { Value = condition.value };
                    if (DateTime.TryParse(condition.value, out var dt))
                    {
                        parameterValue = dt;

                        if (useUtc)
                        {
                            parameterValue = dt.ToUniversalTime();
                            ((StringLiteral)value).Value = dt.ToUniversalTime().ToString("s");
                        }
                    }
                    else
                    {
                        parameterValue = condition.value;
                    }
                }
            }
            else
            {
                value = new StringLiteral { Value = condition.value };
                parameterValue = condition.value;
            }

            if (parameterValue != null && options.UseParametersForLiterals)
            {
                var paramName = $"@{attr.LogicalName}";
                var counter = 0;

                while (parameters.ContainsKey(paramName))
                    paramName = $"@{attr.LogicalName}{++counter}";

                parameters[paramName] = parameterValue;
                value = new VariableReference { Name = paramName };
            }

            // Apply the appropriate conversion for the type of operator
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

                    if (options.ConvertFetchXmlOperatorsTo == FetchXmlOperatorConversion.Functions)
                    {
                        // These FetchXML operators don't have a direct SQL equivalent, so convert to the format
                        // field = function(arg)
                        // so <condition attribute="createdon" operator="lastxdays" value="2" /> will be converted to
                        // createdon = lastxdays(2)

                        type = BooleanComparisonType.Equals;
                        value = new FunctionCall { FunctionName = new Identifier { Value = condition.@operator.ToString() } };

                        if (condition.value != null)
                        {
                            if (Int32.TryParse(condition.value, out _))
                                ((FunctionCall)value).Parameters.Add(new IntegerLiteral { Value = condition.value });
                            else
                                ((FunctionCall)value).Parameters.Add(new StringLiteral { Value = condition.value });
                        }
                        else if (condition.Items != null)
                        {
                            foreach (var conditionValue in condition.Items)
                            {
                                if (Int32.TryParse(conditionValue.Value, out _))
                                    ((FunctionCall)value).Parameters.Add(new IntegerLiteral { Value = conditionValue.Value });
                                else
                                    ((FunctionCall)value).Parameters.Add(new StringLiteral { Value = conditionValue.Value });
                            }
                        }
                    }
                    else
                    {
                        // We want to convert these special FetchXML operators to SQL that a real SQL Server will understand.
                        // This takes some more work to generate the dynamic values to use. Many of these are date/time related,
                        // so store some values for a date range for common reuse later.
                        DateTime? startTime = null;
                        ScalarExpression startExpression = null;
                        DateTime? endTime = null;
                        ScalarExpression endExpression = null;

                        switch (condition.@operator)
                        {
                            case @operator.lastsevendays:
                                startTime = DateTime.Today.AddDays(-7);
                                endTime = DateTime.Now;

                                startExpression = DateAdd("day", new IntegerLiteral { Value = "-7" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new FunctionCall { FunctionName = new Identifier { Value = useUtc ? "@utc_now" : "@now" } };
                                break;

                            case @operator.lastweek:
                                startTime = DateTime.Today.AddDays(-(int)DateTime.Today.DayOfWeek - 7);
                                endTime = startTime.Value.AddDays(7);

                                startExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new UnaryExpression
                                        {
                                            UnaryExpressionType = UnaryExpressionType.Negative,
                                            Expression = new FunctionCall
                                            {
                                                FunctionName = new Identifier { Value = "DATEPART" },
                                                Parameters =
                                                {
                                                    new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "weekday" } } } },
                                                    new VariableReference { Name = "@today" }
                                                }
                                            }
                                        },
                                        SecondExpression = new IntegerLiteral { Value = "6" } // DATEPART(weekday, @today) is 1-based while DateTime.Today.DayOfWeek is 0 based, so use 6 here even though we use 7 above
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );

                                endExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Add,
                                        FirstExpression = new UnaryExpression
                                        {
                                            UnaryExpressionType = UnaryExpressionType.Negative,
                                            Expression = new FunctionCall
                                            {
                                                FunctionName = new Identifier { Value = "DATEPART" },
                                                Parameters =
                                                {
                                                    new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "weekday" } } } },
                                                    new VariableReference { Name = "@today" }
                                                }
                                            }
                                        },
                                        SecondExpression = new IntegerLiteral { Value = "1" }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );

                                break;

                            case @operator.lastmonth:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.Day).AddMonths(-1);
                                endTime = startTime.Value.AddMonths(1);

                                endExpression = DateAdd(
                                        "day",
                                        new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Subtract,
                                            FirstExpression = new IntegerLiteral { Value = "1" },
                                            SecondExpression = new FunctionCall
                                            {
                                                FunctionName = new Identifier { Value = "DATEPART" },
                                                Parameters =
                                                {
                                                    new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = "day" } } } },
                                                    new VariableReference { Name = "@today" }
                                                }
                                            }
                                        },
                                        new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                    );

                                startExpression = DateAdd(
                                    "month",
                                    new IntegerLiteral { Value = "-1" },
                                    endExpression
                                );
                                break;

                            case @operator.lastxdays:
                                startTime = DateTime.Today.AddDays(-Int32.Parse(condition.value, CultureInfo.InvariantCulture));
                                endTime = DateTime.Now;

                                startExpression = DateAdd("day", new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                break;

                            case @operator.lastxhours:
                                startTime = DateTime.Today.AddHours(DateTime.Now.Hour - Int32.Parse(condition.value, CultureInfo.InvariantCulture));
                                endTime = DateTime.Now;

                                startExpression = DateAdd(
                                    "hour",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = DatePart("hour", new VariableReference { Name = "@now" }),
                                        SecondExpression = value
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                break;

                            case @operator.lastxmonths:
                                startTime = DateTime.Today.AddMonths(-Int32.Parse(condition.value, CultureInfo.InvariantCulture));
                                endTime = DateTime.Now;

                                startExpression = DateAdd("month", new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                break;

                            case @operator.lastxweeks:
                                startTime = DateTime.Today.AddDays(-Int32.Parse(condition.value, CultureInfo.InvariantCulture) * 7);
                                endTime = DateTime.Now;

                                startExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Multiply,
                                        FirstExpression = new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value },
                                        SecondExpression = new IntegerLiteral { Value = "7" }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                break;

                            case @operator.lastxyears:
                                startTime = DateTime.Today.AddYears(-Int32.Parse(condition.value, CultureInfo.InvariantCulture));
                                endTime = DateTime.Now;

                                startExpression = DateAdd("year", new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                break;

                            case @operator.lastyear:
                                startTime = new DateTime(DateTime.Today.Year - 1, 1, 1);
                                endTime = startTime.Value.AddYears(1);

                                startExpression = new FunctionCall
                                {
                                    FunctionName = new Identifier { Value = "DATEFROMPARTS" },
                                    Parameters =
                                    {
                                        new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Subtract,
                                            FirstExpression = DatePart("year", new VariableReference { Name = "@today" }),
                                            SecondExpression = new IntegerLiteral { Value = "1" }
                                        },
                                        new IntegerLiteral { Value = "1" },
                                        new IntegerLiteral { Value = "1" }
                                    }
                                };

                                if (useUtc)
                                {
                                    startExpression = DateAdd(
                                        "minute",
                                        new UnaryExpression
                                        {
                                            UnaryExpressionType = UnaryExpressionType.Negative,
                                            Expression = new VariableReference { Name = "@time_zone" }
                                        },
                                        new ConvertCall
                                        {
                                            DataType = new SqlDataTypeReference { SqlDataTypeOption = SqlDataTypeOption.DateTime },
                                            Parameter = startExpression
                                        });
                                }

                                endExpression = DateAdd("year", new IntegerLiteral { Value = "1" }, startExpression);
                                break;

                            case @operator.nextmonth:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.Day).AddMonths(1);
                                endTime = startTime.Value.AddMonths(1);

                                var monthStart = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "1" },
                                        SecondExpression = DatePart("day", new VariableReference { Name = "@today" })
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                startExpression = DateAdd("month", new IntegerLiteral { Value = "1" }, monthStart);
                                endExpression = DateAdd("month", new IntegerLiteral { Value = "2" }, monthStart);
                                break;

                            case @operator.nextsevendays:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(8);

                                startExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                endExpression = DateAdd("day", new IntegerLiteral { Value = "8" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                break;

                            case @operator.nextweek:
                                startTime = DateTime.Today.AddDays(7 - (int)DateTime.Today.DayOfWeek);
                                endTime = startTime.Value.AddDays(7);

                                startExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "8" },  // DATEPART(weekday, @today) is 1-based while DateTime.Today.DayOfWeek is 0 based, so use 8 here even though we use 7 above
                                        SecondExpression = new UnaryExpression
                                        {
                                            UnaryExpressionType = UnaryExpressionType.Negative,
                                            Expression = DatePart("weekday", new VariableReference { Name = "@today" })
                                        }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );

                                endExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "15" },
                                        SecondExpression = new UnaryExpression
                                        {
                                            UnaryExpressionType = UnaryExpressionType.Negative,
                                            Expression = DatePart("weekday", new VariableReference { Name = "@today" })
                                        }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.nextxdays:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(Int32.Parse(condition.value, CultureInfo.InvariantCulture) + 1);

                                startExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                endExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Add,
                                        FirstExpression = value,
                                        SecondExpression = new IntegerLiteral { Value = "1" }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                break;

                            case @operator.nextxhours:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddHours(DateTime.Now.Hour + Int32.Parse(condition.value, CultureInfo.InvariantCulture) + 1);

                                startExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                endExpression = DateAdd(
                                    "hour",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Add,
                                        FirstExpression = DatePart("hour", new VariableReference { Name = useUtc ? "@utc_now" : "@now" }),
                                        SecondExpression = new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Add,
                                            FirstExpression = value,
                                            SecondExpression = new IntegerLiteral { Value = "1" }
                                        }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;
                                
                            case @operator.nextxmonths:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(1).AddMonths(Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                startExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                endExpression = DateAdd(
                                    "month",
                                    value,
                                    DateAdd("day", new IntegerLiteral { Value = "1" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" })
                                );
                                break;

                            case @operator.nextxweeks:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(Int32.Parse(condition.value, CultureInfo.InvariantCulture) * 7 + 1);

                                startExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                endExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Add,
                                        FirstExpression = new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Multiply,
                                            FirstExpression = value,
                                            SecondExpression = new IntegerLiteral { Value = "7" }
                                        },
                                        SecondExpression = new IntegerLiteral { Value = "1" }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.nextxyears:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(1).AddYears(Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                startExpression = new VariableReference { Name = useUtc ? "@utc_now" : "@now" };
                                endExpression = DateAdd(
                                    "year",
                                    value,
                                    DateAdd("day", new IntegerLiteral { Value = "1" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" })
                                );
                                break;

                            case @operator.nextyear:
                                startTime = new DateTime(DateTime.Today.Year + 1, 1, 1);
                                endTime = startTime.Value.AddYears(1);

                                startExpression = new FunctionCall
                                {
                                    FunctionName = new Identifier { Value = "DATEFROMPARTS" },
                                    Parameters =
                                    {
                                        new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Add,
                                            FirstExpression = DatePart("year", new VariableReference { Name = "@today" }),
                                            SecondExpression = new IntegerLiteral { Value = "1" }
                                        },
                                        new IntegerLiteral { Value = "1" },
                                        new IntegerLiteral { Value = "1" }
                                    }
                                };

                                endExpression = new FunctionCall
                                {
                                    FunctionName = new Identifier { Value = "DATEFROMPARTS" },
                                    Parameters =
                                    {
                                        new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Add,
                                            FirstExpression = DatePart("year", new VariableReference { Name = "@today" }),
                                            SecondExpression = new IntegerLiteral { Value = "2" }
                                        },
                                        new IntegerLiteral { Value = "1" },
                                        new IntegerLiteral { Value = "1" }
                                    }
                                };
                                break;

                            case @operator.olderthanxdays:
                                endTime = DateTime.Today.AddDays(-Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                endExpression = DateAdd(
                                    "day",
                                    new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.olderthanxhours:
                                endTime = DateTime.Today.AddHours(DateTime.Now.Hour - Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                endExpression = DateAdd(
                                    "hour",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = DatePart("hour", new VariableReference { Name = "@now" }),
                                        SecondExpression = value
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.olderthanxminutes:
                                endTime = DateTime.Today.AddMinutes(Math.Truncate(DateTime.Now.TimeOfDay.TotalMinutes) - Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                endExpression = DateAdd(
                                    "minute",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new BinaryExpression
                                        {
                                            BinaryExpressionType = BinaryExpressionType.Add,
                                            FirstExpression = new BinaryExpression
                                            {
                                                BinaryExpressionType = BinaryExpressionType.Multiply,
                                                FirstExpression = DatePart("hour", new VariableReference { Name = "@now" }),
                                                SecondExpression = new IntegerLiteral { Value = "60" }
                                            },
                                            SecondExpression = DatePart("minute", new VariableReference { Name = "@now" })
                                        },
                                        SecondExpression = value
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.olderthanxmonths:
                                endTime = DateTime.Today.AddMonths(-Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                endExpression = DateAdd(
                                    "month",
                                    new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.olderthanxweeks:
                                endTime = DateTime.Today.AddDays(-Int32.Parse(condition.value, CultureInfo.InvariantCulture) * 7);

                                endExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Multiply,
                                        FirstExpression = new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value },
                                        SecondExpression = new IntegerLiteral { Value = "7" }
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.olderthanxyears:
                                endTime = DateTime.Today.AddYears(-Int32.Parse(condition.value, CultureInfo.InvariantCulture));

                                endExpression = DateAdd(
                                    "year",
                                    new UnaryExpression { UnaryExpressionType = UnaryExpressionType.Negative, Expression = value },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" }
                                );
                                break;

                            case @operator.on:
                                startTime = DateTime.Parse(condition.value).Date;
                                endTime = startTime.Value.AddDays(1);
                                break;

                            case @operator.onorafter:
                                startTime = DateTime.Parse(condition.value).Date;
                                break;

                            case @operator.onorbefore:
                                endTime = DateTime.Parse(condition.value).Date.AddDays(1);
                                break;

                            case @operator.thismonth:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.Day);
                                endTime = startTime.Value.AddMonths(1);

                                startExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "1" },
                                        SecondExpression = DatePart("day", new VariableReference { Name = "@today" })
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });

                                endExpression = DateAdd("month", new IntegerLiteral { Value = "1" }, startExpression);
                                break;

                            case @operator.thisweek:
                                startTime = DateTime.Today.AddDays(- (int) DateTime.Today.DayOfWeek);
                                endTime = startTime.Value.AddDays(7);

                                startExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "1" },
                                        SecondExpression = DatePart("weekday", new VariableReference { Name = "@today" })
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });

                                endExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "8" },
                                        SecondExpression = DatePart("weekday", new VariableReference { Name = "@today" })
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                break;

                            case @operator.thisyear:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.DayOfYear);
                                endTime = startTime.Value.AddYears(1);

                                startExpression = DateAdd(
                                    "day",
                                    new BinaryExpression
                                    {
                                        BinaryExpressionType = BinaryExpressionType.Subtract,
                                        FirstExpression = new IntegerLiteral { Value = "1" },
                                        SecondExpression = DatePart("dayofyear", new VariableReference { Name = "@today" })
                                    },
                                    new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = DateAdd("year", new IntegerLiteral { Value = "1" }, startExpression);
                                break;

                            case @operator.today:
                                startTime = DateTime.Today;
                                endTime = startTime.Value.AddDays(1);

                                startExpression = new VariableReference { Name = useUtc ? "@utc_today" : "@today" };
                                endExpression = DateAdd("day", new IntegerLiteral { Value = "1" }, startExpression);
                                break;

                            case @operator.tomorrow:
                                startTime = DateTime.Today.AddDays(1);
                                endTime = startTime.Value.AddDays(1);

                                startExpression = DateAdd("day", new IntegerLiteral { Value = "1" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = DateAdd("day", new IntegerLiteral { Value = "2" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                break;

                            case @operator.yesterday:
                                startTime = DateTime.Today.AddDays(-1);
                                endTime = startTime.Value.AddDays(1);

                                startExpression = DateAdd("day", new IntegerLiteral { Value = "-1" }, new VariableReference { Name = useUtc ? "@utc_today" : "@today" });
                                endExpression = new VariableReference { Name = useUtc ? "@utc_today" : "@today" };
                                break;

                            case @operator.eqbusinessid:
                                return new BooleanComparisonExpression
                                {
                                    FirstExpression = field,
                                    ComparisonType = BooleanComparisonType.Equals,
                                    SecondExpression = new StringLiteral { Value = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).BusinessUnitId.ToString("D") }
                                };

                            case @operator.equserid:
                                return new BooleanComparisonExpression
                                {
                                    FirstExpression = field,
                                    ComparisonType = BooleanComparisonType.Equals,
                                    SecondExpression = options.ConvertFetchXmlOperatorsTo == FetchXmlOperatorConversion.Literals ? (ScalarExpression) new StringLiteral { Value = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).UserId.ToString("D") } : new ParameterlessCall { ParameterlessCallType = ParameterlessCallType.CurrentUser }
                                };

                            case @operator.nebusinessid:
                                return new BooleanBinaryExpression
                                {
                                    FirstExpression = new BooleanComparisonExpression
                                    {
                                        FirstExpression = field,
                                        ComparisonType = BooleanComparisonType.NotEqualToBrackets,
                                        SecondExpression = new StringLiteral { Value = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).BusinessUnitId.ToString("D") }
                                    },
                                    BinaryExpressionType = BooleanBinaryExpressionType.Or,
                                    SecondExpression = new BooleanIsNullExpression
                                    {
                                        Expression = field
                                    }
                                };

                            case @operator.neuserid:
                                return new BooleanBinaryExpression
                                {
                                    FirstExpression = new BooleanComparisonExpression
                                    {
                                        FirstExpression = field,
                                        ComparisonType = BooleanComparisonType.NotEqualToBrackets,
                                        SecondExpression = options.ConvertFetchXmlOperatorsTo == FetchXmlOperatorConversion.Literals ? (ScalarExpression) new StringLiteral { Value = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).UserId.ToString("D") } : new ParameterlessCall { ParameterlessCallType = ParameterlessCallType.CurrentUser }
                                    },
                                    BinaryExpressionType = BooleanBinaryExpressionType.Or,
                                    SecondExpression = new BooleanIsNullExpression
                                    {
                                        Expression = field
                                    }
                                };

                            case @operator.equserteams:
                            case @operator.equseroruserteams:
                                {
                                    var userId = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).UserId;
                                    var inTeams = new InPredicate { Expression = field };

                                    if (condition.@operator == @operator.equseroruserteams)
                                        inTeams.Values.Add(new StringLiteral { Value = userId.ToString("D") });

                                    foreach (var teamId in GetUserTeams(org, userId))
                                        inTeams.Values.Add(new StringLiteral { Value = teamId.ToString("D") });

                                    return inTeams;
                                }

                            case @operator.equseroruserhierarchy:
                            case @operator.equseroruserhierarchyandteams:
                                {
                                    var userIds = GetUsersInHierarchy(org).ToList();
                                    var ownerIds = new HashSet<Guid>(userIds);

                                    foreach (var userId in userIds)
                                    {
                                        foreach (var teamId in GetUserTeams(org, userId))
                                            ownerIds.Add(teamId);
                                    }

                                    var inTeams = new InPredicate { Expression = field };

                                    foreach (var ownerId in ownerIds)
                                        inTeams.Values.Add(new StringLiteral { Value = ownerId.ToString("D") });

                                    return inTeams;
                                }

                            case @operator.equserlanguage:
                                {
                                    var inLanguage = new InPredicate { Expression = field };
                                    inLanguage.Values.Add(new IntegerLiteral { Value = GetUserLanguageCode(org).ToString() });
                                    inLanguage.Values.Add(new IntegerLiteral { Value = "-1" });
                                    return inLanguage;
                                }

                            case @operator.thisfiscalyear:
                                {
                                    GetFiscalPeriodSettings(org, out _, out var fiscalStartDate);
                                    startTime = new DateTime(DateTime.Today.Year, fiscalStartDate.Month, fiscalStartDate.Day);

                                    if (startTime.Value < DateTime.Today)
                                    {
                                        endTime = startTime.Value.AddYears(1);
                                    }
                                    else
                                    {
                                        endTime = startTime;
                                        startTime = endTime.Value.AddYears(-1);
                                    }
                                } break;

                            case @operator.thisfiscalperiod:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    startTime = startDate;
                                    endTime = endDate;
                                } break;

                            case @operator.nextfiscalyear:
                                {
                                    GetFiscalPeriodSettings(org, out _, out var fiscalStartDate);
                                    startTime = new DateTime(DateTime.Today.Year + 1, fiscalStartDate.Month, fiscalStartDate.Day);

                                    if (startTime.Value < DateTime.Today)
                                    {
                                        endTime = startTime.Value.AddYears(1);
                                    }
                                    else
                                    {
                                        endTime = startTime;
                                        startTime = endTime.Value.AddYears(-1);
                                    }
                                } break;

                            case @operator.nextfiscalperiod:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    startTime = endDate;
                                    endTime = AddFiscalPeriod(endDate, fiscalPeriodType);
                                }
                                break;

                            case @operator.lastfiscalyear:
                                {
                                    GetFiscalPeriodSettings(org, out _, out var fiscalStartDate);
                                    startTime = new DateTime(DateTime.Today.Year - 1, fiscalStartDate.Month, fiscalStartDate.Day);

                                    if (startTime.Value < DateTime.Today)
                                    {
                                        endTime = startTime.Value.AddYears(1);
                                    }
                                    else
                                    {
                                        endTime = startTime;
                                        startTime = endTime.Value.AddYears(-1);
                                    }
                                }
                                break;

                            case @operator.lastfiscalperiod:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    startTime = SubtractFiscalPeriod(startDate, fiscalPeriodType);
                                    endTime = startDate;
                                }
                                break;

                            case @operator.lastxfiscalyears:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out _);

                                    startTime = new DateTime(fiscalYear - Int32.Parse(condition.value, CultureInfo.InvariantCulture), fiscalStartDate.Month, fiscalStartDate.Day);
                                    endTime = DateTime.Now;
                                }
                                break;

                            case @operator.lastxfiscalperiods:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    for (var i = 0; i < Int32.Parse(condition.value, CultureInfo.InvariantCulture); i++)
                                        startDate = SubtractFiscalPeriod(startDate, fiscalPeriodType);

                                    startTime = startDate;
                                    endTime = DateTime.Now;
                                }
                                break;

                            case @operator.nextxfiscalyears:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out _);

                                    startTime = DateTime.Now;
                                    endTime = new DateTime(fiscalYear + Int32.Parse(condition.value, CultureInfo.InvariantCulture) + 1, fiscalStartDate.Month, fiscalStartDate.Day);
                                }
                                break;

                            case @operator.nextxfiscalperiods:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    for (var i = 0; i < Int32.Parse(condition.value, CultureInfo.InvariantCulture); i++)
                                        endDate = AddFiscalPeriod(endDate, fiscalPeriodType);

                                    startTime = DateTime.Now;
                                    endTime = endDate;
                                }
                                break;

                            case @operator.infiscalyear:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);

                                    startTime = new DateTime(Int32.Parse(condition.value, CultureInfo.InvariantCulture), fiscalStartDate.Month, fiscalStartDate.Day);
                                    endTime = startTime.Value.AddYears(1);
                                }
                                break;

                            case @operator.infiscalperiod:
                                // This requires the use of a scalar valued function in the target SQL database to get the fiscal period from each
                                // date in order to check it.
                                throw new NotSupportedException("infiscalperiod condition operator cannot be converted to native SQL");

                            case @operator.infiscalperiodandyear:
                                {
                                    var values = condition.Items.OfType<conditionValue>().Select(v => Int32.Parse(v.Value, CultureInfo.InvariantCulture)).ToArray();
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, values[1], values[0], out var startDate, out var endDate);

                                    startTime = startDate;
                                    endTime = endDate;
                                }
                                break;

                            case @operator.inorbeforefiscalperiodandyear:
                                {
                                    var values = condition.Items.OfType<conditionValue>().Select(v => Int32.Parse(v.Value, CultureInfo.InvariantCulture)).ToArray();
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, values[1], values[0], out _, out var endDate);

                                    endTime = endDate;
                                }
                                break;

                            case @operator.inorafterfiscalperiodandyear:
                                {
                                    var values = condition.Items.OfType<conditionValue>().Select(v => Int32.Parse(v.Value, CultureInfo.InvariantCulture)).ToArray();
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, values[1], values[0], out var startDate, out _);

                                    startTime = startDate;
                                }
                                break;

                            case @operator.under:
                            case @operator.eqorunder:
                            case @operator.notunder:
                                {
                                    var cte = GetUnderCte(meta, new Guid(condition.value), ctes, condition.@operator == @operator.under);
                                    var inPred = new InPredicate
                                    {
                                        Expression = field,
                                        Subquery = new ScalarSubquery
                                        {
                                            QueryExpression = new QuerySpecification
                                            {
                                                SelectElements =
                                                {
                                                    new SelectScalarExpression
                                                    {
                                                        Expression = new ColumnReferenceExpression
                                                        {
                                                            MultiPartIdentifier = new MultiPartIdentifier
                                                            {
                                                                Identifiers =
                                                                {
                                                                    new Identifier { Value = meta.PrimaryIdAttribute }
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                FromClause = new FromClause
                                                {
                                                    TableReferences =
                                                    {
                                                        new NamedTableReference
                                                        {
                                                            SchemaObject = new SchemaObjectName
                                                            {
                                                                Identifiers =
                                                                {
                                                                    new Identifier { Value = cte.ExpressionName.Value }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    };

                                    if (condition.@operator == @operator.notunder)
                                    {
                                        inPred.NotDefined = true;

                                        return new BooleanBinaryExpression
                                        {
                                            FirstExpression = new BooleanIsNullExpression { Expression = field },
                                            BinaryExpressionType = BooleanBinaryExpressionType.Or,
                                            SecondExpression = inPred
                                        };
                                    }

                                    return inPred;
                                }

                            case @operator.above:
                            case @operator.eqorabove:
                                {
                                    var cte = GetAboveCte(meta, new Guid(condition.value), ctes, condition.@operator == @operator.above);
                                    var inPred = new InPredicate
                                    {
                                        Expression = field,
                                        Subquery = new ScalarSubquery
                                        {
                                            QueryExpression = new QuerySpecification
                                            {
                                                SelectElements =
                                                {
                                                    new SelectScalarExpression
                                                    {
                                                        Expression = new ColumnReferenceExpression
                                                        {
                                                            MultiPartIdentifier = new MultiPartIdentifier
                                                            {
                                                                Identifiers =
                                                                {
                                                                    new Identifier { Value = meta.PrimaryIdAttribute }
                                                                }
                                                            }
                                                        }
                                                    }
                                                },
                                                FromClause = new FromClause
                                                {
                                                    TableReferences =
                                                    {
                                                        new NamedTableReference
                                                        {
                                                            SchemaObject = new SchemaObjectName
                                                            {
                                                                Identifiers =
                                                                {
                                                                    new Identifier { Value = cte.ExpressionName.Value }
                                                                }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    };

                                    return inPred;
                                }

                            case @operator.containvalues:
                            case @operator.notcontainvalues:
                                {
                                    BooleanExpression contains = new FullTextPredicate
                                    {
                                        Columns = { field },
                                        FullTextFunctionType = FullTextFunctionType.Contains,
                                        Value = new StringLiteral { Value = String.Join(" OR ", condition.Items.Select(val => val.Value)) }
                                    };

                                    if (condition.@operator == @operator.notcontainvalues)
                                        contains = new BooleanNotExpression { Expression = contains };

                                    return contains;
                                }

                            default:
                                throw new NotSupportedException($"Conversion of the {condition.@operator} FetchXML operator to native SQL is not currently supported");
                        }

                        BooleanExpression expr = null;

                        if (startTime != null)
                        {
                            if (useUtc)
                                startTime = startTime.Value.ToUniversalTime();

                            var useExpression = options.ConvertFetchXmlOperatorsTo == FetchXmlOperatorConversion.SqlCalculations && startExpression != null;

                            expr = new BooleanComparisonExpression
                            {
                                FirstExpression = field,
                                ComparisonType = BooleanComparisonType.GreaterThanOrEqualTo,
                                SecondExpression = useExpression ? startExpression : new StringLiteral { Value = startTime.Value.ToString("s") }
                            };

                            usesToday |= useExpression;
                        }

                        if (endTime != null)
                        {
                            if (useUtc)
                                endTime = endTime.Value.ToUniversalTime();

                            var useExpression = options.ConvertFetchXmlOperatorsTo == FetchXmlOperatorConversion.SqlCalculations && endExpression != null;

                            var endExpr = new BooleanComparisonExpression
                            {
                                FirstExpression = field,
                                ComparisonType = BooleanComparisonType.LessThan,
                                SecondExpression = useExpression ? endExpression : new StringLiteral { Value = endTime.Value.ToString("s") }
                            };

                            usesToday |= useExpression;

                            if (expr == null)
                            {
                                expr = endExpr;
                            }
                            else
                            {
                                expr = new BooleanBinaryExpression
                                {
                                    FirstExpression = expr,
                                    BinaryExpressionType = BooleanBinaryExpressionType.And,
                                    SecondExpression = endExpr
                                };
                            }
                        }

                        return expr;
                    }

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
                    throw new NotSupportedException($"Conversion of the {condition.@operator} FetchXML operator to SQL is not currently supported");
            }

            var expression = new BooleanComparisonExpression
            {
                FirstExpression = field,
                ComparisonType = type,
                SecondExpression = value
            };

            return expression;
        }

        private static FunctionCall DateAdd(string datePart, ScalarExpression number, ScalarExpression date)
        {
            return new FunctionCall
            {
                FunctionName = new Identifier { Value = "DATEADD" },
                Parameters =
                {
                    new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = datePart } } } },
                    number,
                    date
                }
            };
        }

        private static FunctionCall DatePart(string datePart, ScalarExpression date)
        {
            return new FunctionCall
            {
                FunctionName = new Identifier { Value = "DATEPART" },
                Parameters =
                {
                    new ColumnReferenceExpression { MultiPartIdentifier = new MultiPartIdentifier { Identifiers = { new Identifier { Value = datePart } } } },
                    date
                }
            };
        }

        /// <summary>
        /// Generates a Common Table Expression to recurse down a hierarchy from a given record
        /// </summary>
        /// <param name="meta">The metadata of the entity to recurse in</param>
        /// <param name="guid">The unique identifier of the starting record to recurse down from</param>
        /// <param name="ctes">The details of all the CTEs already in the query</param>
        /// <param name="excludeAnchor">Indicates if the starting record should be excluded from the results</param>
        /// <returns>The CTE that represents the required query</returns>
        /// <remarks>
        /// Generates a CTE like:
        /// 
        /// account_hierarchical(AccountId) AS 
        /// (
        /// SELECT accountid FROM account WHERE accountid = 'guid'
        /// UNION ALL
        /// SELECT account.accountid FROM account INNER JOIN account_hierarchical ON account.parentaccountid = account_hierarchical.accountid
        /// </remarks>
        private static CommonTableExpression GetUnderCte(EntityMetadata meta, Guid guid, IDictionary<string, CommonTableExpression> ctes, bool excludeAnchor)
        {
            return GetCte(meta, guid, false, ctes, excludeAnchor);
        }

        /// <summary>
        /// Generates a Common Table Expression to recurse up a hierarchy from a given record
        /// </summary>
        /// <param name="meta">The metadata of the entity to recurse in</param>
        /// <param name="guid">The unique identifier of the starting record to recurse down from</param>
        /// <param name="ctes">The details of all the CTEs already in the query</param>
        /// <param name="excludeAnchor">Indicates if the starting record should be excluded from the results</param>
        /// <returns>The CTE that represents the required query</returns>
        /// <remarks>
        /// Generates a CTE like:
        /// 
        /// account_hierarchical(AccountId, ParentAccountId) AS 
        /// (
        /// SELECT accountid, parentaccountid FROM account WHERE accountid = 'guid'
        /// UNION ALL
        /// SELECT account.accountid, account.parentaccountid FROM account INNER JOIN account_hierarchical ON account.accountid = account_hierarchical.parentaccountid
        /// </remarks>
        private static CommonTableExpression GetAboveCte(EntityMetadata meta, Guid guid, IDictionary<string, CommonTableExpression> ctes, bool excludeAnchor)
        {
            return GetCte(meta, guid, true, ctes, excludeAnchor);
        }

        /// <summary>
        /// Generates a Common Table Expression to recurse up or down a hierarchy from a given record
        /// </summary>
        /// <param name="meta">The metadata of the entity to recurse in</param>
        /// <param name="guid">The unique identifier of the starting record to recurse down from</param>
        /// <param name="above">Indicates if the CTE should find records above the selected record (<c>true</c>) or below (<c>false</c>)</param>
        /// <param name="ctes">The details of all the CTEs already in the query</param>
        /// <returns>The CTE that represents the required query</returns>
        private static CommonTableExpression GetCte(EntityMetadata meta, Guid guid, bool above, IDictionary<string, CommonTableExpression> ctes, bool excludeAnchor)
        {
            if (meta == null)
                throw new DisconnectedException();

            var hierarchyRelationship = meta.ManyToOneRelationships.SingleOrDefault(r => r.IsHierarchical == true);

            if (hierarchyRelationship == null)
                throw new NotSupportedException("No hierarchical relationship defined for " + meta.LogicalName);

            var parentLookupAttribute = hierarchyRelationship.ReferencingAttribute;

            var baseName = $"{meta.LogicalName}_hierarchical";
            var name = baseName;
            var counter = 0;

            while (ctes.ContainsKey(name))
            {
                name = baseName + counter;
                counter++;
            }

            var cte = new CommonTableExpression
            {
                ExpressionName = new Identifier { Value = name },
                Columns =
                {
                    new Identifier { Value = meta.PrimaryIdAttribute },
                    new Identifier { Value = parentLookupAttribute }
                },
                QueryExpression = new BinaryQueryExpression
                {
                    FirstQueryExpression = new QuerySpecification
                    {
                        SelectElements =
                        {
                            new SelectScalarExpression
                            {
                                Expression = new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = meta.PrimaryIdAttribute }
                                        }
                                    }
                                }
                            },
                            new SelectScalarExpression
                            {
                                Expression = new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = parentLookupAttribute }
                                        }
                                    }
                                }
                            }
                        },
                        FromClause = new FromClause
                        {
                            TableReferences =
                            {
                                new NamedTableReference
                                {
                                    SchemaObject = new SchemaObjectName
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = meta.LogicalName }
                                        }
                                    }
                                }
                            }
                        },
                        WhereClause = new WhereClause
                        {
                            SearchCondition = new BooleanComparisonExpression
                            {
                                FirstExpression = new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = excludeAnchor && !above ? parentLookupAttribute : meta.PrimaryIdAttribute }
                                        }
                                    }
                                },
                                ComparisonType = BooleanComparisonType.Equals,
                                SecondExpression = new StringLiteral { Value = guid.ToString("D") }
                            }
                        }
                    },
                    BinaryQueryExpressionType = BinaryQueryExpressionType.Union,
                    All = true,
                    SecondQueryExpression = new QuerySpecification
                    {
                        SelectElements =
                        {
                            new SelectScalarExpression
                            {
                                Expression = new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = meta.LogicalName },
                                            new Identifier { Value = meta.PrimaryIdAttribute }
                                        }
                                    }
                                }
                            },
                            new SelectScalarExpression
                            {
                                Expression = new ColumnReferenceExpression
                                {
                                    MultiPartIdentifier = new MultiPartIdentifier
                                    {
                                        Identifiers =
                                        {
                                            new Identifier { Value = meta.LogicalName },
                                            new Identifier { Value = parentLookupAttribute }
                                        }
                                    }
                                }
                            }
                        },
                        FromClause = new FromClause
                        {
                            TableReferences =
                            {
                                new QualifiedJoin
                                {
                                    FirstTableReference = new NamedTableReference
                                    {
                                        SchemaObject = new SchemaObjectName
                                        {
                                            Identifiers =
                                            {
                                                new Identifier { Value = meta.LogicalName }
                                            }
                                        }
                                    },
                                    QualifiedJoinType = QualifiedJoinType.Inner,
                                    SecondTableReference = new NamedTableReference
                                    {
                                        SchemaObject = new SchemaObjectName
                                        {
                                            Identifiers =
                                            {
                                                new Identifier { Value = name }
                                            }
                                        }
                                    },
                                    SearchCondition = new BooleanComparisonExpression
                                    {
                                        FirstExpression = new ColumnReferenceExpression
                                        {
                                            MultiPartIdentifier = new MultiPartIdentifier
                                            {
                                                Identifiers =
                                                {
                                                    new Identifier { Value = meta.LogicalName },
                                                    new Identifier { Value = above ? meta.PrimaryIdAttribute : parentLookupAttribute }
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
                                                    new Identifier { Value = name },
                                                    new Identifier { Value = above ? parentLookupAttribute : meta.PrimaryIdAttribute }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            };

            if (excludeAnchor && above)
            {
                // Need to include a join in the anchor query to find the records above the source record but not including that record
                var query = (BinaryQueryExpression)cte.QueryExpression;
                var anchorQuery = (QuerySpecification)query.FirstQueryExpression;
                var filter = (BooleanComparisonExpression)anchorQuery.WhereClause.SearchCondition;
                var field = (ColumnReferenceExpression)filter.FirstExpression;

                anchorQuery.FromClause.TableReferences[0] = new QualifiedJoin
                {
                    FirstTableReference = anchorQuery.FromClause.TableReferences[0],
                    QualifiedJoinType = QualifiedJoinType.Inner,
                    SecondTableReference = new NamedTableReference
                    {
                        SchemaObject = new SchemaObjectName
                        {
                            Identifiers =
                            {
                                new Identifier { Value = meta.LogicalName }
                            }
                        },
                        Alias = new Identifier { Value = "anchor" }
                    },
                    SearchCondition = new BooleanComparisonExpression
                    {
                        FirstExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = meta.LogicalName },
                                    new Identifier { Value = meta.PrimaryIdAttribute }
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
                                    new Identifier { Value = "anchor" },
                                    new Identifier { Value = parentLookupAttribute }
                                }
                            }
                        }
                    }
                };

                ((ColumnReferenceExpression)((SelectScalarExpression)anchorQuery.SelectElements[0]).Expression).MultiPartIdentifier.Identifiers.Insert(0, new Identifier { Value = meta.LogicalName });
                ((ColumnReferenceExpression)((SelectScalarExpression)anchorQuery.SelectElements[1]).Expression).MultiPartIdentifier.Identifiers.Insert(0, new Identifier { Value = meta.LogicalName });
                field.MultiPartIdentifier.Identifiers.Insert(0, new Identifier { Value = "anchor" });
            }

            if (!above)
            {
                // Don't need the parent attribute in the CTE for "under" queries
                cte.Columns.RemoveAt(1);

                var query = (BinaryQueryExpression)cte.QueryExpression;
                var anchorQuery = (QuerySpecification)query.FirstQueryExpression;
                var recursiveQuery = (QuerySpecification)query.SecondQueryExpression;

                anchorQuery.SelectElements.RemoveAt(1);
                recursiveQuery.SelectElements.RemoveAt(1);
            }

            ctes.Add(name, cte);

            return cte;
        }

        enum FiscalPeriodType
        {
            Annually = 2000,
            SemiAnnually = 2001,
            Quarterly = 2002,
            Monthly = 2003,
            FourWeekly = 2004
        }

        private static void GetFiscalPeriodSettings(IOrganizationService org, out FiscalPeriodType type, out DateTime fiscalStartDate)
        {
            if (org == null)
                throw new DisconnectedException();

            var qry = new Microsoft.Xrm.Sdk.Query.QueryExpression("organization");
            qry.ColumnSet = new ColumnSet("fiscalperiodtype", "fiscalcalendarstart");

            var result = org.RetrieveMultiple(qry).Entities.Single();

            type = (FiscalPeriodType)result.GetAttributeValue<int>("fiscalperiodtype");
            fiscalStartDate = result.GetAttributeValue<DateTime>("fiscalcalendarstart");
        }

        private static void GetFiscalPeriodNumber(FiscalPeriodType type, DateTime fiscalStartDate, DateTime date, out int fiscalYear, out int fiscalPeriod)
        {
            var yearStart = new DateTime(date.Year, fiscalStartDate.Month, fiscalStartDate.Day);

            if (yearStart > date)
                yearStart = yearStart.AddYears(-1);

            fiscalYear = yearStart.Year;
            fiscalPeriod = 1;
            var periodStart = yearStart;
            var periodEnd = AddFiscalPeriod(periodStart, type);

            while (periodEnd < date)
            {
                fiscalPeriod++;
                periodEnd = AddFiscalPeriod(periodEnd, type);
            }
        }

        private static DateTime AddFiscalPeriod(DateTime startDate, FiscalPeriodType type)
        {
            switch (type)
            {
                case FiscalPeriodType.Annually:
                    return startDate.AddYears(1);

                case FiscalPeriodType.SemiAnnually:
                    return startDate.AddMonths(6);

                case FiscalPeriodType.Quarterly:
                    return startDate.AddMonths(3);

                case FiscalPeriodType.Monthly:
                    return startDate.AddMonths(1);

                case FiscalPeriodType.FourWeekly:
                    return startDate.AddDays(7 * 4);

                default:
                    throw new NotSupportedException("Unknown fiscal period type");
            }
        }

        private static DateTime SubtractFiscalPeriod(DateTime startDate, FiscalPeriodType type)
        {
            switch (type)
            {
                case FiscalPeriodType.Annually:
                    return startDate.AddYears(-1);

                case FiscalPeriodType.SemiAnnually:
                    return startDate.AddMonths(-6);

                case FiscalPeriodType.Quarterly:
                    return startDate.AddMonths(-3);

                case FiscalPeriodType.Monthly:
                    return startDate.AddMonths(-1);

                case FiscalPeriodType.FourWeekly:
                    return startDate.AddDays(-7 * 4);

                default:
                    throw new NotSupportedException("Unknown fiscal period type");
            }
        }

        private static void GetFiscalPeriodDates(DateTime fiscalStartDate, FiscalPeriodType type, int yearNumber, int periodNumber, out DateTime startDate, out DateTime endDate)
        {
            var fiscalYearStart = new DateTime(yearNumber, fiscalStartDate.Month, fiscalStartDate.Day);
            startDate = fiscalYearStart;
            endDate = AddFiscalPeriod(startDate, type);

            for (var i = 1; i < periodNumber; i++)
            {
                startDate = endDate;
                endDate = AddFiscalPeriod(startDate, type);
            }
        }

        private static IEnumerable<Guid> GetUsersInHierarchy(IOrganizationService org)
        {
            if (org == null)
                throw new DisconnectedException();

            var qry = new Microsoft.Xrm.Sdk.Query.QueryExpression("systemuser");
            qry.ColumnSet = new ColumnSet("systemuserid");
            qry.Criteria.AddCondition("systemuserid", ConditionOperator.EqualUserOrUserHierarchy);

            var results = org.RetrieveMultiple(qry).Entities;

            return results.Select(e => e.Id);
        }

        private static IEnumerable<Guid> GetUserTeams(IOrganizationService org, Guid userId)
        {
            if (org == null)
                throw new DisconnectedException();

            var qry = new Microsoft.Xrm.Sdk.Query.QueryExpression("teammembership");
            qry.ColumnSet = new ColumnSet("teamid");
            qry.Criteria.AddCondition("systemuserid", ConditionOperator.EqualUserId);

            var results = org.RetrieveMultiple(qry).Entities;

            return results.Select(e => e.GetAttributeValue<Guid>("teamid"));
        }

        private static int GetUserLanguageCode(IOrganizationService org)
        {
            if (org == null)
                throw new DisconnectedException();

            var qry = new Microsoft.Xrm.Sdk.Query.QueryExpression("usersetting");
            qry.ColumnSet = new ColumnSet("uilanguageid");
            qry.Criteria.AddCondition("systemuserid", ConditionOperator.EqualUserId);

            var result = org.RetrieveMultiple(qry).Entities.FirstOrDefault();

            if (result == null)
                return -1;

            return result.GetAttributeValue<int>("uilanguageid");
        }

        /// <summary>
        /// Converts a FetchXML &lt;order&gt; element to the SQL equivalent
        /// </summary>
        /// <param name="name">The name or alias of the &lt;entity&gt; or &lt;link-entity&gt; that the sorts are from</param>
        /// <param name="items">The items within the &lt;entity&gt; or &lt;link-entity&gt; to take the sorts from</param>
        /// <param name="query">The SQL query to apply the sorts to</param>
        private static void AddOrderBy(string name, object[] items, QuerySpecification query, bool useRawOrderBy, IAttributeMetadataCache metadata, Dictionary<string,string> aliasToLogicalName)
        {
            if (items == null)
                return;

            // Find any sorts within the entity
            foreach (var sort in items.OfType<FetchOrderType>())
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
                                    new Identifier { Value = sort.alias }
                                }
                            }
                        },
                        SortOrder = sort.descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
                else
                {
                    var entityAlias = sort.entityname ?? name;
                    var attributeName = sort.attribute;

                    if (!aliasToLogicalName.TryGetValue(entityAlias, out var entityLogicalName))
                        entityLogicalName = entityAlias;

                    var entityMetadata = metadata[entityLogicalName];
                    var attr = entityMetadata.Attributes.SingleOrDefault(a => a.LogicalName == attributeName);

                    if (attr is LookupAttributeMetadata || ((attr is EnumAttributeMetadata || attr is BooleanAttributeMetadata) && !useRawOrderBy))
                        attributeName += "name";

                    query.OrderByClause.OrderByElements.Add(new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers =
                                {
                                    new Identifier { Value = entityAlias },
                                    new Identifier { Value = attributeName }
                                }
                            }
                        },
                        SortOrder = sort.descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
            }

            // Recurse into link entities
            foreach (var link in items.OfType<FetchLinkEntityType>())
                AddOrderBy(link.alias ?? link.name, link.Items, query, useRawOrderBy, metadata, aliasToLogicalName);
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

            public override void ExplicitVisit(ExistsPredicate node)
            {
            }
        }

        private class QuoteIdentifiersVisitor : TSqlFragmentVisitor
        {
            public override void ExplicitVisit(Identifier node)
            {
                node.QuoteType = DatabaseIdentifiers.RequiresQuote(node.Value) ? QuoteType.SquareBracket : QuoteType.NotQuoted;
                base.ExplicitVisit(node);
            }
        }
    }

    public class FetchXml2SqlOptions
    {
        public FetchXmlOperatorConversion ConvertFetchXmlOperatorsTo { get; set; } = FetchXmlOperatorConversion.Functions;

        public bool UseParametersForLiterals { get; set; } = false;

        public bool ConvertDateTimeToUtc { get; set; }
    }

    public enum FetchXmlOperatorConversion
    {
        Functions,
        Literals,
        SqlCalculations
    }
}
