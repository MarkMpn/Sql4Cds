using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
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
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="fetch">The FetchXML string to convert</param>
        /// <returns>The converted SQL query</returns>
        public static string Convert(IAttributeMetadataCache metadata, string fetch, FetchXml2SqlOptions options, out IDictionary<string,object> parameterValues)
        {
            using (var stream = new MemoryStream(Encoding.UTF8.GetBytes(fetch)))
            {
                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));
                var parsed = (FetchXml.FetchType)serializer.Deserialize(stream);

                return Convert(metadata, parsed, options, out parameterValues);
            }
        }

        /// <summary>
        /// Converts a FetchXML query to SQL
        /// </summary>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="fetch">The query object to convert</param>
        /// <returns>The converted SQL query</returns>
        public static string Convert(IAttributeMetadataCache metadata, FetchXml.FetchType fetch, FetchXml2SqlOptions options, out IDictionary<string,object> parameterValues)
        {
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
                query.FromClause.TableReferences[0] = BuildJoins(metadata, query.FromClause.TableReferences[0], (NamedTableReference)query.FromClause.TableReferences[0], entity.Items, query, aliasToLogicalName, fetch.nolock, options);
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
            var filter = GetFilter(metadata, entity.Items, entity.name, aliasToLogicalName, options);
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
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="dataSource">The current data source of the SQL query</param>
        /// <param name="parentTable">The details of the table that this new table is being linked to</param>
        /// <param name="items">The FetchXML items in this entity</param>
        /// <param name="query">The current state of the SQL query being built</param>
        /// <param name="aliasToLogicalName">A mapping of table aliases to the logical name</param>
        /// <param name="nolock">Indicates if the NOLOCK table hint should be applied</param>
        /// <returns>The data source including any required joins</returns>
        private static TableReference BuildJoins(IAttributeMetadataCache metadata, TableReference dataSource, NamedTableReference parentTable, object[] items, QuerySpecification query, IDictionary<string, string> aliasToLogicalName, bool nolock, FetchXml2SqlOptions options)
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
                var filter = GetFilter(metadata, link.Items, link.alias ?? link.name, aliasToLogicalName, options);
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

                // Recurse into any other links
                dataSource = BuildJoins(metadata, join, (NamedTableReference)join.SecondTableReference, link.Items, query, aliasToLogicalName, nolock, options);
            }

            return dataSource;
        }

        /// <summary>
        /// Converts a FetchXML &lt;filter&gt; to a SQL condition
        /// </summary>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="items">The items in the &lt;entity&gt; or &lt;link-entity&gt; to process the &lt;filter&gt; from</param>
        /// <param name="prefix">The alias or name of the table that the &lt;filter&gt; applies to</param>
        /// <param name="aliasToLogicalName">The mapping of table alias to logical name</param>
        /// <returns>The SQL condition equivalent of the &lt;filter&gt; found in the <paramref name="items"/>, or <c>null</c> if no filter was found</returns>
        private static BooleanExpression GetFilter(IAttributeMetadataCache metadata, object[] items, string prefix, IDictionary<string, string> aliasToLogicalName, FetchXml2SqlOptions options)
        {
            if (items == null)
                return null;

            var filter = items.OfType<filter>().SingleOrDefault();

            if (filter == null)
                return null;

            return GetFilter(metadata, filter, prefix, aliasToLogicalName, options);
        }

        /// <summary>
        /// Converts a FetchXML &lt;filter&gt; to a SQL condition
        /// </summary>
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="filter">The FetchXML filter to convert</param>
        /// <param name="prefix">The alias or name of the table that the <paramref name="filter"/> applies to</param>
        /// <param name="aliasToLogicalName">The mapping of table alias to logical name</param>
        /// <returns>The SQL condition equivalent of the <paramref name="filter"/></returns>
        private static BooleanExpression GetFilter(IAttributeMetadataCache metadata, filter filter, string prefix, IDictionary<string, string> aliasToLogicalName, FetchXml2SqlOptions options)
        {
            BooleanExpression expression = null;
            var type = filter.type == filterType.and ? BooleanBinaryExpressionType.And : BooleanBinaryExpressionType.Or;

            // Convert each <condition> within the filter
            foreach (var condition in filter.Items.OfType<condition>())
            {
                var newExpression = GetCondition(metadata, condition, prefix, aliasToLogicalName, options);

                if (expression == null)
                {
                    expression = newExpression;
                }
                else
                {
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
                var newExpression = GetFilter(metadata, subFilter, prefix, aliasToLogicalName, options);

                if (expression == null)
                {
                    expression = newExpression;
                }
                else
                {
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
        /// <param name="metadata">The metadata cache to use for the conversion</param>
        /// <param name="condition">The FetchXML condition to convert</param>
        /// <param name="prefix">The alias or name of the table that the <paramref name="condition"/> applies to</param>
        /// <param name="aliasToLogicalName">The mapping of table alias to logical name</param>
        /// <returns>The SQL condition equivalent of the <paramref name="condition"/></returns>
        private static BooleanExpression GetCondition(IAttributeMetadataCache metadata, condition condition, string prefix, IDictionary<string,string> aliasToLogicalName, FetchXml2SqlOptions options)
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

            var meta = metadata[logicalName];
            var attr = meta.Attributes.SingleOrDefault(a => a.LogicalName == condition.attribute);

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
            else if (attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Lookup ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Owner ||
                attr.AttributeType == Microsoft.Xrm.Sdk.Metadata.AttributeTypeCode.Customer)
                value = new IdentifierLiteral { Value = condition.value };
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
                    }
                    else
                    {
                        DateTime? startTime = null;
                        DateTime? endTime = null;

                        switch (condition.@operator)
                        {
                            case @operator.lastsevendays:
                            case @operator.lastweek:
                                startTime = DateTime.Today.AddDays(-7);
                                endTime = DateTime.Now;
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
                                startTime = DateTime.Today.AddYears(-1);
                                endTime = DateTime.Now;
                                break;

                            case @operator.nextmonth:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(1).AddMonths(1);
                                break;

                            case @operator.nextsevendays:
                            case @operator.nextweek:
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(8);
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
                                startTime = DateTime.Now;
                                endTime = DateTime.Today.AddDays(1).AddYears(1);
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

                            default:
                                throw new NotImplementedException();
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
