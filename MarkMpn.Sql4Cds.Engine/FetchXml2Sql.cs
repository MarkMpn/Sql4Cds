using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;
using Microsoft.Xrm.Sdk.Query;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
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
            var select = new SelectStatement();
            var query = new QuerySpecification();
            select.QueryExpression = query;

            if (fetch.top != null)
                query.TopRowFilter = new TopRowFilter { Expression = new IntegerLiteral { Value = fetch.top } };

            if (fetch.distinct)
                query.UniqueRowFilter = UniqueRowFilter.Distinct;

            // SELECT (columns from first table)
            var entity = fetch.Items.OfType<FetchEntityType>().SingleOrDefault();
            AddSelectElements(query, entity.Items, entity?.name);

            // FROM
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

                if (fetch.nolock)
                    ((NamedTableReference)query.FromClause.TableReferences[0]).TableHints.Add(new TableHint { HintKind = TableHintKind.NoLock });

                // Recurse into link-entities to build joins
                query.FromClause.TableReferences[0] = BuildJoins(org, metadata, query.FromClause.TableReferences[0], (NamedTableReference)query.FromClause.TableReferences[0], entity.Items, query, aliasToLogicalName, fetch.nolock, options, ctes);
            }

            // OFFSET
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

            // WHERE
            var filter = GetFilter(org, metadata, entity.Items, entity.name, aliasToLogicalName, options, ctes);
            if (filter != null)
            {
                query.WhereClause = new WhereClause
                {
                    SearchCondition = filter
                };
            }

            // ORDER BY
            AddOrderBy(entity.name, entity.Items, query);

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

            // Optionally convert literal values to parameters
            parameterValues = null;

            if (options.UseParametersForLiterals)
            {
                parameterValues = new Dictionary<string, object>();
                select.Accept(new LiteralsToParametersVisitor(parameterValues));
            }

            new Sql150ScriptGenerator().GenerateScript(select, out var sql);

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
        private static TableReference BuildJoins(IOrganizationService org, IAttributeMetadataCache metadata, TableReference dataSource, NamedTableReference parentTable, object[] items, QuerySpecification query, IDictionary<string, string> aliasToLogicalName, bool nolock, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes)
        {
            if (items == null)
                return dataSource;

            // Find any <link-entity> elements to process
            foreach (var link in items.OfType<FetchLinkEntityType>())
            {
                // Store the alias of this link
                if (!String.IsNullOrEmpty(link.alias))
                    aliasToLogicalName[link.alias] = link.name;

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

                if (nolock)
                    table.TableHints.Add(new TableHint { HintKind = TableHintKind.NoLock });

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
                var filter = GetFilter(org, metadata, link.Items, link.alias ?? link.name, aliasToLogicalName, options, ctes);

                if (filter != null)
                {
                    if (!(filter is BooleanBinaryExpression bbe) || bbe.BinaryExpressionType == BooleanBinaryExpressionType.And)
                    {
                        join.SearchCondition = new BooleanBinaryExpression
                        {
                            FirstExpression = join.SearchCondition,
                            BinaryExpressionType = BooleanBinaryExpressionType.And,
                            SecondExpression = filter
                        };
                    }
                    else
                    {
                        join.SearchCondition = new BooleanBinaryExpression
                        {
                            FirstExpression = new BooleanParenthesisExpression { Expression = join.SearchCondition },
                            BinaryExpressionType = BooleanBinaryExpressionType.And,
                            SecondExpression = new BooleanParenthesisExpression { Expression = filter }
                        };
                    }
                }

                // Recurse into any other links
                dataSource = BuildJoins(org, metadata, join, (NamedTableReference)join.SecondTableReference, link.Items, query, aliasToLogicalName, nolock, options, ctes);
            }

            return dataSource;
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
        private static BooleanExpression GetFilter(IOrganizationService org, IAttributeMetadataCache metadata, object[] items, string prefix, IDictionary<string, string> aliasToLogicalName, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes)
        {
            if (items == null)
                return null;

            var filters = items.OfType<filter>().ToList();

            BooleanExpression expression = null;

            foreach (var filter in filters)
            {
                var newExpression = GetFilter(org, metadata, filter, prefix, aliasToLogicalName, options, ctes);

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
        private static BooleanExpression GetFilter(IOrganizationService org, IAttributeMetadataCache metadata, filter filter, string prefix, IDictionary<string, string> aliasToLogicalName, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes)
        {
            BooleanExpression expression = null;
            var type = filter.type == filterType.and ? BooleanBinaryExpressionType.And : BooleanBinaryExpressionType.Or;

            // Convert each <condition> within the filter
            foreach (var condition in filter.Items.OfType<condition>())
            {
                var newExpression = GetCondition(org, metadata, condition, prefix, aliasToLogicalName, options, ctes);

                if (newExpression is BooleanBinaryExpression bbe && bbe.BinaryExpressionType != type)
                    newExpression = new BooleanParenthesisExpression { Expression = newExpression };

                if (expression == null)
                {
                    expression = newExpression;
                }
                else
                {
                    if (expression is BooleanBinaryExpression lhs && lhs.BinaryExpressionType != type)
                        expression = new BooleanParenthesisExpression { Expression = expression };

                    expression = new BooleanBinaryExpression
                    {
                        FirstExpression = expression,
                        BinaryExpressionType = type,
                        SecondExpression = newExpression
                    };
                }
            }

            // Recurse into sub-<filter>s
            foreach (var subFilter in filter.Items.OfType<filter>())
            {
                var newExpression = GetFilter(org, metadata, subFilter, prefix, aliasToLogicalName, options, ctes);

                if (newExpression is BooleanBinaryExpression bbe && bbe.BinaryExpressionType != type)
                    newExpression = new BooleanParenthesisExpression { Expression = newExpression };

                if (expression == null)
                {
                    expression = newExpression;
                }
                else
                {
                    if (expression is BooleanBinaryExpression lhs && lhs.BinaryExpressionType != type)
                        expression = new BooleanParenthesisExpression { Expression = expression };

                    expression = new BooleanBinaryExpression
                    {
                        FirstExpression = expression,
                        BinaryExpressionType = type,
                        SecondExpression = newExpression
                    };
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
        private static BooleanExpression GetCondition(IOrganizationService org, IAttributeMetadataCache metadata, condition condition, string prefix, IDictionary<string,string> aliasToLogicalName, FetchXml2SqlOptions options, IDictionary<string, CommonTableExpression> ctes)
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

            // Get the literal value to compare to
            if (!String.IsNullOrEmpty(condition.valueof))
                value = new ColumnReferenceExpression
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
            else if (attr == null)
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
            else
                value = new StringLiteral { Value = condition.value };

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

                    if (options.PreserveFetchXmlOperatorsAsFunctions)
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
                        DateTime? endTime = null;

                        switch (condition.@operator)
                        {
                            case @operator.lastsevendays:
                                startTime = DateTime.Today.AddDays(-7);
                                endTime = DateTime.Now;
                                break;

                            case @operator.lastweek:
                                startTime = DateTime.Today.AddDays(-(int)DateTime.Today.DayOfWeek - 7);
                                endTime = startTime.Value.AddDays(7);
                                break;

                            case @operator.lastmonth:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.Day).AddMonths(-1);
                                endTime = startTime.Value.AddMonths(1);
                                break;

                            case @operator.lastxdays:
                                startTime = DateTime.Today.AddDays(-Int32.Parse(condition.value));
                                endTime = DateTime.Now;
                                break;

                            case @operator.lastxhours:
                                startTime = DateTime.Today.AddHours(DateTime.Now.Hour - Int32.Parse(condition.value));
                                endTime = DateTime.Now;
                                break;

                            case @operator.lastxmonths:
                                startTime = DateTime.Today.AddMonths(-Int32.Parse(condition.value));
                                endTime = DateTime.Now;
                                break;

                            case @operator.lastxweeks:
                                startTime = DateTime.Today.AddDays(-Int32.Parse(condition.value) * 7);
                                endTime = DateTime.Now;
                                break;

                            case @operator.lastxyears:
                                startTime = DateTime.Today.AddYears(-Int32.Parse(condition.value));
                                endTime = DateTime.Now;
                                break;

                            case @operator.lastyear:
                                startTime = new DateTime(DateTime.Today.Year, 1, 1);
                                endTime = startTime.Value.AddYears(1);
                                break;

                            case @operator.nextmonth:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.Day).AddMonths(1);
                                endTime = startTime.Value.AddMonths(1);
                                break;

                            case @operator.nextsevendays:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(8);
                                break;

                            case @operator.nextweek:
                                startTime = DateTime.Today.AddDays(7 - (int)DateTime.Today.DayOfWeek);
                                endTime = startTime.Value.AddDays(7);
                                break;

                            case @operator.nextxdays:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(Int32.Parse(condition.value) + 1);
                                break;

                            case @operator.nextxhours:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddHours(DateTime.Now.Hour + Int32.Parse(condition.value) + 1);
                                break;
                                
                            case @operator.nextxmonths:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(1).AddMonths(Int32.Parse(condition.value));
                                break;

                            case @operator.nextxweeks:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(Int32.Parse(condition.value) * 7 + 1);
                                break;

                            case @operator.nextxyears:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(1).AddYears(Int32.Parse(condition.value));
                                break;

                            case @operator.nextyear:
                                startTime = new DateTime(DateTime.Today.Year + 1, 1, 1);
                                endTime = startTime.Value.AddYears(1);
                                break;

                            case @operator.olderthanxdays:
                                endTime = DateTime.Today.AddDays(-Int32.Parse(condition.value));
                                break;

                            case @operator.olderthanxhours:
                                endTime = DateTime.Today.AddHours(DateTime.Now.Hour - Int32.Parse(condition.value));
                                break;

                            case @operator.olderthanxminutes:
                                endTime = DateTime.Today.AddMinutes(Math.Truncate(DateTime.Now.TimeOfDay.TotalMinutes) - Int32.Parse(condition.value));
                                break;

                            case @operator.olderthanxmonths:
                                endTime = DateTime.Today.AddMonths(-Int32.Parse(condition.value));
                                break;

                            case @operator.olderthanxweeks:
                                endTime = DateTime.Today.AddDays(-Int32.Parse(condition.value) * 7);
                                break;

                            case @operator.olderthanxyears:
                                endTime = DateTime.Today.AddYears(-Int32.Parse(condition.value));
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
                                break;

                            case @operator.thisweek:
                                startTime = DateTime.Today.AddDays(- (int) DateTime.Today.DayOfWeek);
                                endTime = startTime.Value.AddDays(7);
                                break;

                            case @operator.thisyear:
                                startTime = DateTime.Today.AddDays(1 - DateTime.Today.DayOfYear);
                                endTime = startTime.Value.AddYears(1);
                                break;

                            case @operator.today:
                                startTime = DateTime.Today;
                                endTime = startTime.Value.AddDays(1);
                                break;

                            case @operator.tomorrow:
                                startTime = DateTime.Today.AddDays(1);
                                endTime = startTime.Value.AddDays(1);
                                break;

                            case @operator.yesterday:
                                startTime = DateTime.Today.AddDays(-1);
                                endTime = startTime.Value.AddDays(1);
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
                                    SecondExpression = new StringLiteral { Value = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).UserId.ToString("D") }
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
                                        SecondExpression = new StringLiteral { Value = ((WhoAmIResponse)org.Execute(new WhoAmIRequest())).UserId.ToString("D") }
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

                                    startTime = new DateTime(fiscalYear - Int32.Parse(condition.value), fiscalStartDate.Month, fiscalStartDate.Day);
                                    endTime = DateTime.Now;
                                }
                                break;

                            case @operator.lastxfiscalperiods:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    for (var i = 0; i < Int32.Parse(condition.value); i++)
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
                                    endTime = new DateTime(fiscalYear + Int32.Parse(condition.value) + 1, fiscalStartDate.Month, fiscalStartDate.Day);
                                }
                                break;

                            case @operator.nextxfiscalperiods:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodNumber(fiscalPeriodType, fiscalStartDate, DateTime.Today, out var fiscalYear, out var fiscalPeriod);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, fiscalYear, fiscalPeriod, out var startDate, out var endDate);

                                    for (var i = 0; i < Int32.Parse(condition.value); i++)
                                        endDate = AddFiscalPeriod(endDate, fiscalPeriodType);

                                    startTime = DateTime.Now;
                                    endTime = endDate;
                                }
                                break;

                            case @operator.infiscalyear:
                                {
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);

                                    startTime = new DateTime(Int32.Parse(condition.value), fiscalStartDate.Month, fiscalStartDate.Day);
                                    endTime = startTime.Value.AddYears(1);
                                }
                                break;

                            case @operator.infiscalperiod:
                                // This requires the use of a scalar valued function in the target SQL database to get the fiscal period from each
                                // date in order to check it.
                                throw new NotSupportedException("infiscalperiod condition operator cannot be converted to native SQL");

                            case @operator.infiscalperiodandyear:
                                {
                                    var values = condition.Items.OfType<conditionValue>().Select(v => Int32.Parse(v.Value)).ToArray();
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, values[1], values[0], out var startDate, out var endDate);

                                    startTime = startDate;
                                    endTime = endDate;
                                }
                                break;

                            case @operator.inorbeforefiscalperiodandyear:
                                {
                                    var values = condition.Items.OfType<conditionValue>().Select(v => Int32.Parse(v.Value)).ToArray();
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, values[1], values[0], out _, out var endDate);

                                    endTime = endDate;
                                }
                                break;

                            case @operator.inorafterfiscalperiodandyear:
                                {
                                    var values = condition.Items.OfType<conditionValue>().Select(v => Int32.Parse(v.Value)).ToArray();
                                    GetFiscalPeriodSettings(org, out var fiscalPeriodType, out var fiscalStartDate);
                                    GetFiscalPeriodDates(fiscalStartDate, fiscalPeriodType, values[1], values[0], out var startDate, out _);

                                    startTime = startDate;
                                }
                                break;

                            case @operator.under:
                            case @operator.eqorunder:
                            case @operator.notunder:
                                {
                                    var cte = GetUnderCte(meta, new Guid(condition.value), ctes);
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
                                                                    new Identifier { Value = cte.ExpressionName.Value },
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
                                                },
                                                WhereClause = condition.@operator != @operator.under ? null : new WhereClause
                                                {
                                                    SearchCondition = new BooleanComparisonExpression
                                                    {
                                                        FirstExpression = new ColumnReferenceExpression
                                                        {
                                                            MultiPartIdentifier = new MultiPartIdentifier
                                                            {
                                                                Identifiers =
                                                                {
                                                                    new Identifier { Value = cte.ExpressionName.Value },
                                                                    new Identifier { Value = "Level" }
                                                                }
                                                            }
                                                        },
                                                        ComparisonType = BooleanComparisonType.NotEqualToBrackets,
                                                        SecondExpression = new IntegerLiteral { Value = "0" }
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
                                    var cte = GetAboveCte(meta, new Guid(condition.value), ctes);
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
                                                                    new Identifier { Value = cte.ExpressionName.Value },
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
                                                },
                                                WhereClause = condition.@operator != @operator.above ? null : new WhereClause
                                                {
                                                    SearchCondition = new BooleanComparisonExpression
                                                    {
                                                        FirstExpression = new ColumnReferenceExpression
                                                        {
                                                            MultiPartIdentifier = new MultiPartIdentifier
                                                            {
                                                                Identifiers =
                                                                {
                                                                    new Identifier { Value = cte.ExpressionName.Value },
                                                                    new Identifier { Value = "Level" }
                                                                }
                                                            }
                                                        },
                                                        ComparisonType = BooleanComparisonType.NotEqualToBrackets,
                                                        SecondExpression = new IntegerLiteral { Value = "0" }
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
                            expr = new BooleanComparisonExpression
                            {
                                FirstExpression = field,
                                ComparisonType = BooleanComparisonType.GreaterThanOrEqualTo,
                                SecondExpression = new StringLiteral { Value = startTime.Value.ToString("s") }
                            };
                        }

                        if (endTime != null)
                        {
                            var endExpr = new BooleanComparisonExpression
                            {
                                FirstExpression = field,
                                ComparisonType = BooleanComparisonType.LessThan,
                                SecondExpression = new StringLiteral { Value = endTime.Value.ToString("s") }
                            };

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

        /// <summary>
        /// Generates a Common Table Expression to recurse down a hierarchy from a given record
        /// </summary>
        /// <param name="meta">The metadata of the entity to recurse in</param>
        /// <param name="guid">The unique identifier of the starting record to recurse down from</param>
        /// <param name="ctes">The details of all the CTEs already in the query</param>
        /// <returns>The CTE that represents the required query</returns>
        /// <remarks>
        /// Generates a CTE like:
        /// 
        /// account_hierarchical([Level], AccountId, ParentAccountId) AS 
        /// (
        /// SELECT 0, accountid, parentaccountid FROM account WHERE accountid = 'guid'
        /// UNION ALL
        /// SELECT Level + 1, account.accountid, account.parentaccountid FROM account INNER JOIN account_hierarchical ON account.parentaccountid = account_hierarchical.accountid
        /// </remarks>
        private static CommonTableExpression GetUnderCte(EntityMetadata meta, Guid guid, IDictionary<string, CommonTableExpression> ctes)
        {
            return GetCte(meta, guid, false, ctes);
        }

        /// <summary>
        /// Generates a Common Table Expression to recurse up a hierarchy from a given record
        /// </summary>
        /// <param name="meta">The metadata of the entity to recurse in</param>
        /// <param name="guid">The unique identifier of the starting record to recurse down from</param>
        /// <param name="ctes">The details of all the CTEs already in the query</param>
        /// <returns>The CTE that represents the required query</returns>
        /// <remarks>
        /// Generates a CTE like:
        /// 
        /// account_hierarchical([Level], AccountId, ParentAccountId) AS 
        /// (
        /// SELECT 0, accountid, parentaccountid FROM account WHERE accountid = 'guid'
        /// UNION ALL
        /// SELECT Level + 1, account.accountid, account.parentaccountid FROM account INNER JOIN account_hierarchical ON account.accountid = account_hierarchical.parentaccountid
        /// </remarks>
        private static CommonTableExpression GetAboveCte(EntityMetadata meta, Guid guid, IDictionary<string, CommonTableExpression> ctes)
        {
            return GetCte(meta, guid, true, ctes);
        }

        /// <summary>
        /// Generates a Common Table Expression to recurse up or down a hierarchy from a given record
        /// </summary>
        /// <param name="meta">The metadata of the entity to recurse in</param>
        /// <param name="guid">The unique identifier of the starting record to recurse down from</param>
        /// <param name="above">Indicates if the CTE should find records above the selected record (<c>true</c>) or below (<c>false</c>)</param>
        /// <param name="ctes">The details of all the CTEs already in the query</param>
        /// <returns>The CTE that represents the required query</returns>
        private static CommonTableExpression GetCte(EntityMetadata meta, Guid guid, bool above, IDictionary<string, CommonTableExpression> ctes)
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
                    new Identifier { Value = "Level" },
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
                                Expression = new IntegerLiteral { Value = "0" }
                            },
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
                                            new Identifier { Value = meta.PrimaryIdAttribute }
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
                                Expression = new BinaryExpression
                                {
                                    FirstExpression = new ColumnReferenceExpression
                                    {
                                        MultiPartIdentifier = new MultiPartIdentifier
                                        {
                                            Identifiers =
                                            {
                                                new Identifier { Value = "Level" }
                                            }
                                        }
                                    },
                                    BinaryExpressionType = BinaryExpressionType.Add,
                                    SecondExpression = new IntegerLiteral { Value = "1" }
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
        private static void AddOrderBy(string name, object[] items, QuerySpecification query)
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
                                new Identifier{Value = name},
                                new Identifier{Value = sort.attribute}
                            }
                            }
                        },
                        SortOrder = sort.descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
            }

            // Recurse into link entities
            foreach (var link in items.OfType<FetchLinkEntityType>())
                AddOrderBy(link.alias ?? link.name, link.Items, query);
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
            private static readonly string[] ReservedWords = new[]
            {
                "ADD",
                "ALL",
                "ALTER",
                "AND",
                "ANY",
                "AS",
                "ASC",
                "AUTHORIZATION",
                "BACKUP",
                "BEGIN",
                "BETWEEN",
                "BREAK",
                "BROWSE",
                "BULK",
                "BY",
                "CASCADE",
                "CASE",
                "CHECK",
                "CHECKPOINT",
                "CLOSE",
                "CLUSTERED",
                "COALESCE",
                "COLLATE",
                "COLUMN",
                "COMMIT",
                "COMPUTE",
                "CONSTRAINT",
                "CONTAINS",
                "CONTAINSTABLE",
                "CONTINUE",
                "CONVERT",
                "CREATE",
                "CROSS",
                "CURRENT",
                "CURRENT_DATE",
                "CURRENT_TIME",
                "CURRENT_TIMESTAMP",
                "CURRENT_USER",
                "CURSOR",
                "DATABASE",
                "DBCC",
                "DEALLOCATE",
                "DECLARE",
                "DEFAULT",
                "DELETE",
                "DENY",
                "DESC",
                "DISK",
                "DISTINCT",
                "DISTRIBUTED",
                "DOUBLE",
                "DROP",
                "DUMP",
                "ELSE",
                "END",
                "ERRLVL",
                "ESCAPE",
                "EXCEPT",
                "EXEC",
                "EXECUTE",
                "EXISTS",
                "EXIT",
                "EXTERNAL",
                "FETCH",
                "FILE",
                "FILLFACTOR",
                "FOR",
                "FOREIGN",
                "FREETEXT",
                "FREETEXTTABLE",
                "FROM",
                "FULL",
                "FUNCTION",
                "GOTO",
                "GRANT",
                "GROUP",
                "HAVING",
                "HOLDLOCK",
                "IDENTITY",
                "IDENTITY_INSERT",
                "IDENTITYCOL",
                "IF",
                "IN",
                "INDEX",
                "INNER",
                "INSERT",
                "INTERSECT",
                "INTO",
                "IS",
                "JOIN",
                "KEY",
                "KILL",
                "LEFT",
                "LIKE",
                "LINENO",
                "LOAD",
                "MERGE",
                "NATIONAL",
                "NOCHECK",
                "NONCLUSTERED",
                "NOT",
                "NULL",
                "NULLIF",
                "OF",
                "OFF",
                "OFFSETS",
                "ON",
                "OPEN",
                "OPENDATASOURCE",
                "OPENQUERY",
                "OPENROWSET",
                "OPENXML",
                "OPTION",
                "OR",
                "ORDER",
                "OUTER",
                "OVER",
                "PERCENT",
                "PIVOT",
                "PLAN",
                "PRECISION",
                "PRIMARY",
                "PRINT",
                "PROC",
                "PROCEDURE",
                "PUBLIC",
                "RAISERROR",
                "READ",
                "READTEXT",
                "RECONFIGURE",
                "REFERENCES",
                "REPLICATION",
                "RESTORE",
                "RESTRICT",
                "RETURN",
                "REVERT",
                "REVOKE",
                "RIGHT",
                "ROLLBACK",
                "ROWCOUNT",
                "ROWGUIDCOL",
                "RULE",
                "SAVE",
                "SCHEMA",
                "SECURITYAUDIT",
                "SELECT",
                "SEMANTICKEYPHRASETABLE",
                "SEMANTICSIMILARITYDETAILSTABLE",
                "SEMANTICSIMILARITYTABLE",
                "SESSION_USER",
                "SET",
                "SETUSER",
                "SHUTDOWN",
                "SOME",
                "STATISTICS",
                "SYSTEM_USER",
                "TABLE",
                "TABLESAMPLE",
                "TEXTSIZE",
                "THEN",
                "TO",
                "TOP",
                "TRAN",
                "TRANSACTION",
                "TRIGGER",
                "TRUNCATE",
                "TRY_CONVERT",
                "TSEQUAL",
                "UNION",
                "UNIQUE",
                "UNPIVOT",
                "UPDATE",
                "UPDATETEXT",
                "USE",
                "USER",
                "VALUES",
                "VARYING",
                "VIEW",
                "WAITFOR",
                "WHEN",
                "WHERE",
                "WHILE",
                "WITH",
                "WITHIN GROUP",
                "WRITETEXT"
            };

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

        private class LiteralsToParametersVisitor : TSqlFragmentVisitor
        {
            private readonly IDictionary<string, object> _parameters;

            public LiteralsToParametersVisitor(IDictionary<string, object> parameters)
            {
                _parameters = parameters;
            }

            public override void ExplicitVisit(BooleanComparisonExpression node)
            {
                base.ExplicitVisit(node);

                if (node.FirstExpression is ColumnReferenceExpression col &&
                    node.SecondExpression is Literal literal)
                {
                    var parameterName = GetParameterName(col.MultiPartIdentifier.Identifiers.Last().Value);
                    var param = new VariableReference { Name = parameterName };
                    node.SecondExpression = param;
                    _parameters[parameterName] = GetParameterValue(literal);
                }
            }

            public override void ExplicitVisit(InPredicate node)
            {
                base.ExplicitVisit(node);

                if (node.Expression is ColumnReferenceExpression col)
                {
                    for (var i = 0; i < node.Values.Count; i++)
                    {
                        if (node.Values[i] is Literal literal)
                        {
                            var parameterName = GetParameterName(col.MultiPartIdentifier.Identifiers.Last().Value);
                            var param = new VariableReference { Name = parameterName };
                            node.Values[i] = param;
                            _parameters[parameterName] = GetParameterValue(literal);
                        }
                    }
                }
            }

            public override void ExplicitVisit(BooleanTernaryExpression node)
            {
                base.ExplicitVisit(node);

                if (node.FirstExpression is ColumnReferenceExpression col)
                {
                    if (node.SecondExpression is Literal lit1)
                    {
                        var parameterName = GetParameterName(col.MultiPartIdentifier.Identifiers.Last().Value);
                        var param = new VariableReference { Name = parameterName };
                        node.SecondExpression = param;
                        _parameters[parameterName] = GetParameterValue(lit1);
                    }

                    if (node.ThirdExpression is Literal lit2)
                    {
                        var parameterName = GetParameterName(col.MultiPartIdentifier.Identifiers.Last().Value);
                        var param = new VariableReference { Name = parameterName };
                        node.ThirdExpression = param;
                        _parameters[parameterName] = GetParameterValue(lit2);
                    }
                }
            }

            public override void ExplicitVisit(LikePredicate node)
            {
                base.ExplicitVisit(node);

                if (node.FirstExpression is ColumnReferenceExpression col &&
                    node.SecondExpression is Literal literal)
                {
                    var parameterName = GetParameterName(col.MultiPartIdentifier.Identifiers.Last().Value);
                    var param = new VariableReference { Name = parameterName };
                    node.SecondExpression = param;
                    _parameters[parameterName] = GetParameterValue(literal);
                }
            }

            private object GetParameterValue(Literal literal)
            {
                if (literal is IntegerLiteral i)
                    return Int32.Parse(i.Value);

                if (literal is MoneyLiteral m)
                    return Decimal.Parse(m.Value);

                if (literal is NumericLiteral n)
                    return Decimal.Parse(n.Value);

                return literal.Value;
            }

            private string GetParameterName(string column)
            {
                var baseName = "@" + column;

                if (!_parameters.ContainsKey(baseName))
                    return baseName;

                var suffix = 1;

                while (_parameters.ContainsKey(baseName + suffix))
                    suffix++;

                return baseName + suffix;
            }
        }
    }

    public class FetchXml2SqlOptions
    {
        public bool PreserveFetchXmlOperatorsAsFunctions { get; set; } = true;

        public bool UseParametersForLiterals { get; set; } = false;
    }
}
