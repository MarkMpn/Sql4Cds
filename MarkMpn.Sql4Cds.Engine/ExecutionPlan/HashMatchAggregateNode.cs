using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.ServiceModel;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using MarkMpn.Sql4Cds.Engine.QueryExtensions;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Produces aggregate values using a hash table for grouping
    /// </summary>
    class HashMatchAggregateNode : BaseAggregateNode
    {
        private bool _folded;

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues)
        {
            var groups = new Dictionary<GroupingKey, Dictionary<string, AggregateFunction>>();
            var schema = Source.GetSchema(dataSources, parameterTypes);
            var groupByCols = GetGroupingColumns(schema);

            InitializeAggregates(schema, parameterTypes);

            foreach (var entity in Source.Execute(dataSources, options, parameterTypes, parameterValues))
            {
                var key = new GroupingKey(entity, groupByCols);

                if (!groups.TryGetValue(key, out var values))
                {
                    values = CreateGroupValues(parameterValues, options, false);
                    groups[key] = values;
                }

                foreach (var func in values.Values)
                    func.NextRecord(entity);
            }

            foreach (var group in groups)
            {
                var result = new Entity();

                for (var i = 0; i < GroupBy.Count; i++)
                    result[groupByCols[i]] = group.Key.Values[i];

                foreach (var aggregate in group.Value)
                    result[aggregate.Key] = aggregate.Value.Value;

                yield return result;
            }
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            if (_folded)
                return this;

            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;

            // Special case for using RetrieveTotalRecordCount instead of FetchXML
            if (options.UseRetrieveTotalRecordCount &&
                Source is FetchXmlScan fetch &&
                (fetch.Entity.Items == null || fetch.Entity.Items.Length == 0) &&
                GroupBy.Count == 0 &&
                Aggregates.Count == 1 &&
                Aggregates.Single().Value.AggregateType == AggregateType.CountStar &&
                dataSources[fetch.DataSource].Metadata[fetch.Entity.name].DataProviderId == null) // RetrieveTotalRecordCountRequest is not valid for virtual entities
            {
                var count = new RetrieveTotalRecordCountNode { DataSource = fetch.DataSource, EntityName = fetch.Entity.name };
                var countName = count.GetSchema(dataSources, parameterTypes).Schema.Single().Key;

                if (countName == Aggregates.Single().Key)
                    return count;

                var rename = new ComputeScalarNode
                {
                    Source = count,
                    Columns =
                    {
                        [Aggregates.Single().Key] = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = countName } }
                            }
                        }
                    }
                };
                count.Parent = rename;

                return rename;
            }

            if (Source is FetchXmlScan || Source is ComputeScalarNode computeScalar && computeScalar.Source is FetchXmlScan)
            {
                // Check if all the aggregates & groupings can be done in FetchXML. Can only convert them if they can ALL
                // be handled - if any one needs to be calculated manually, we need to calculate them all. Also track if
                // we can partition the query for larger source data sets. We can't partition DISTINCT aggregates, and need
                // to transform AVG(field) to SUM(field) / COUNT(field)
                var canPartition = true;

                foreach (var agg in Aggregates)
                {
                    if (agg.Value.SqlExpression != null && !(agg.Value.SqlExpression is ColumnReferenceExpression))
                        return this;

                    if (agg.Value.Distinct && agg.Value.AggregateType != ExecutionPlan.AggregateType.Count)
                        return this;

                    if (agg.Value.AggregateType == AggregateType.First)
                        return this;

                    if (agg.Value.Distinct)
                        canPartition = false;
                }

                var fetchXml = Source as FetchXmlScan;
                computeScalar = Source as ComputeScalarNode;

                var partnames = new Dictionary<string, FetchXml.DateGroupingType>(StringComparer.OrdinalIgnoreCase)
                {
                    ["year"] = DateGroupingType.year,
                    ["yy"] = DateGroupingType.year,
                    ["yyyy"] = DateGroupingType.year,
                    ["quarter"] = DateGroupingType.quarter,
                    ["qq"] = DateGroupingType.quarter,
                    ["q"] = DateGroupingType.quarter,
                    ["month"] = DateGroupingType.month,
                    ["mm"] = DateGroupingType.month,
                    ["m"] = DateGroupingType.month,
                    ["day"] = DateGroupingType.day,
                    ["dd"] = DateGroupingType.day,
                    ["d"] = DateGroupingType.day,
                    ["week"] = DateGroupingType.week,
                    ["wk"] = DateGroupingType.week,
                    ["ww"] = DateGroupingType.week
                };

                if (computeScalar != null)
                {
                    fetchXml = (FetchXmlScan)computeScalar.Source;

                    // Groupings may be on DATEPART function, which will have been split into separate Compute Scalar node. Check if all the scalar values
                    // being computed are DATEPART functions that can be converted to FetchXML and are used as groupings
                    foreach (var scalar in computeScalar.Columns)
                    {
                        if (!(scalar.Value is FunctionCall func) ||
                            !func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) ||
                            func.Parameters.Count != 2 ||
                            !(func.Parameters[0] is ColumnReferenceExpression datePartType) ||
                            !(func.Parameters[1] is ColumnReferenceExpression datePartCol))
                            return this;

                        if (!GroupBy.Any(g => g.MultiPartIdentifier.Identifiers.Count == 1 && g.MultiPartIdentifier.Identifiers[0].Value == scalar.Key))
                            return this;

                        if (!partnames.ContainsKey(datePartType.GetColumnName()))
                            return this;
                    }
                }

                var metadata = dataSources[fetchXml.DataSource].Metadata;

                // FetchXML is translated to QueryExpression for virtual entities, which doesn't support aggregates
                if (metadata[fetchXml.Entity.name].DataProviderId != null)
                    return this;

                // Check none of the grouped columns are virtual attributes - FetchXML doesn't support grouping by them
                var fetchSchema = fetchXml.GetSchema(dataSources, parameterTypes);
                foreach (var group in GroupBy)
                {
                    if (!fetchSchema.ContainsColumn(group.GetColumnName(), out var groupCol))
                        continue;

                    var parts = groupCol.Split('.');
                    string entityName;

                    if (parts[0] == fetchXml.Alias)
                        entityName = fetchXml.Entity.name;
                    else
                        entityName = fetchXml.Entity.FindLinkEntity(parts[0]).name;

                    var attr = metadata[entityName].Attributes.SingleOrDefault(a => a.LogicalName == parts[1]);

                    if (attr == null || attr.AttributeOf != null)
                        return this;
                }

                // FetchXML aggregates can trigger an AggregateQueryRecordLimitExceeded error. Clone the non-aggregate FetchXML
                // so we can try to run the native aggregate version but fall back to in-memory processing where necessary
                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));

                var clonedFetchXml = new FetchXmlScan
                {
                    DataSource = fetchXml.DataSource,
                    Alias = fetchXml.Alias,
                    AllPages = fetchXml.AllPages,
                    FetchXml = (FetchXml.FetchType)serializer.Deserialize(new StringReader(fetchXml.FetchXmlString)),
                    ReturnFullSchema = fetchXml.ReturnFullSchema
                };

                if (Source == fetchXml)
                {
                    Source = clonedFetchXml;
                    clonedFetchXml.Parent = this;
                }
                else
                {
                    computeScalar.Source = clonedFetchXml;
                    clonedFetchXml.Parent = computeScalar;
                }

                fetchXml.FetchXml.aggregate = true;
                fetchXml.FetchXml.aggregateSpecified = true;
                fetchXml.FetchXml = fetchXml.FetchXml;

                var schema = Source.GetSchema(dataSources, parameterTypes);

                foreach (var grouping in GroupBy)
                {
                    var colName = grouping.GetColumnName();
                    var alias = grouping.MultiPartIdentifier.Identifiers.Last().Value;
                    DateGroupingType? dateGrouping = null;

                    if (computeScalar != null && computeScalar.Columns.TryGetValue(colName, out var datePart))
                    {
                        dateGrouping = partnames[((ColumnReferenceExpression)((FunctionCall)datePart).Parameters[0]).GetColumnName()];
                        colName = ((ColumnReferenceExpression)((FunctionCall)datePart).Parameters[1]).GetColumnName();
                    }

                    schema.ContainsColumn(colName, out colName);

                    var attribute = fetchXml.AddAttribute(colName, a => a.groupbySpecified && a.groupby == FetchBoolType.@true && a.alias == alias, metadata, out _, out var linkEntity);
                    attribute.groupby = FetchBoolType.@true;
                    attribute.groupbySpecified = true;
                    attribute.alias = alias;

                    if (dateGrouping != null)
                    {
                        attribute.dategrouping = dateGrouping.Value;
                        attribute.dategroupingSpecified = true;
                    }
                    else if (grouping.GetType(schema, null, parameterTypes) == typeof(SqlDateTime))
                    {
                        // Can't group on datetime columns without a DATEPART specification
                        return this;
                    }

                    // Add a sort order for each grouping to allow consistent paging
                    var items = linkEntity?.Items ?? fetchXml.Entity.Items;
                    var sort = items.OfType<FetchOrderType>().FirstOrDefault(order => order.alias == alias);
                    if (sort == null)
                    {
                        if (linkEntity == null)
                            fetchXml.Entity.AddItem(new FetchOrderType { alias = alias });
                        else
                            linkEntity.AddItem(new FetchOrderType { alias = alias });
                    }
                }

                foreach (var agg in Aggregates)
                {
                    var col = (ColumnReferenceExpression)agg.Value.SqlExpression;
                    var colName = col == null ? (fetchXml.Alias + "." + metadata[fetchXml.Entity.name].PrimaryIdAttribute) : col.GetColumnName();

                    if (!schema.ContainsColumn(colName, out colName))
                        return this;

                    var distinct = agg.Value.Distinct ? FetchBoolType.@true : FetchBoolType.@false;

                    FetchXml.AggregateType aggregateType;

                    switch (agg.Value.AggregateType)
                    {
                        case ExecutionPlan.AggregateType.Average:
                            aggregateType = FetchXml.AggregateType.avg;
                            break;

                        case ExecutionPlan.AggregateType.Count:
                            aggregateType = FetchXml.AggregateType.countcolumn;
                            break;

                        case ExecutionPlan.AggregateType.CountStar:
                            aggregateType = FetchXml.AggregateType.count;
                            break;

                        case ExecutionPlan.AggregateType.Max:
                            aggregateType = FetchXml.AggregateType.max;
                            break;

                        case ExecutionPlan.AggregateType.Min:
                            aggregateType = FetchXml.AggregateType.min;
                            break;

                        case ExecutionPlan.AggregateType.Sum:
                            aggregateType = FetchXml.AggregateType.sum;
                            break;

                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    // min, max, sum and avg are not supported for optionset attributes
                    var parts = colName.Split('.');
                    string entityName;

                    if (parts[0] == fetchXml.Alias)
                        entityName = fetchXml.Entity.name;
                    else
                        entityName = fetchXml.Entity.FindLinkEntity(parts[0]).name;

                    var attr = metadata[entityName].Attributes.SingleOrDefault(a => a.LogicalName == parts[1]);

                    if (attr == null)
                        return this;

                    if (attr is EnumAttributeMetadata && (aggregateType == FetchXml.AggregateType.avg || aggregateType == FetchXml.AggregateType.max || aggregateType == FetchXml.AggregateType.min || aggregateType == FetchXml.AggregateType.sum))
                        return this;

                    var attribute = fetchXml.AddAttribute(colName, a => a.aggregate == aggregateType && a.alias == agg.Key && a.distinct == distinct, metadata, out _, out _);
                    attribute.aggregate = aggregateType;
                    attribute.aggregateSpecified = true;
                    attribute.alias = agg.Key;

                    if (agg.Value.Distinct)
                    {
                        attribute.distinct = distinct;
                        attribute.distinctSpecified = true;
                    }
                }

                // FoldQuery can be called again in some circumstances. Don't repeat the folding operation and create another try/catch
                _folded = true;

                IDataExecutionPlanNode firstTry = fetchXml;

                // If the main aggregate query fails due to having over 50K records, check if we can retry with partitioning. We
                // need a createdon field to be available for this to work.
                if (canPartition)
                    canPartition = metadata[fetchXml.Entity.name].Attributes.Any(a => a.LogicalName == "createdon");

                if (canPartition)
                {
                    // Create a clone of the aggregate FetchXML query
                    var partitionedFetchXml = new FetchXmlScan
                    {
                        DataSource = fetchXml.DataSource,
                        Alias = fetchXml.Alias,
                        AllPages = fetchXml.AllPages,
                        FetchXml = (FetchXml.FetchType)serializer.Deserialize(new StringReader(fetchXml.FetchXmlString)),
                        ReturnFullSchema = fetchXml.ReturnFullSchema
                    };

                    var partitionedAggregates = new PartitionedAggregateNode
                    {
                        Source = partitionedFetchXml
                    };
                    partitionedFetchXml.Parent = partitionedAggregates;
                    var partitionedResults = (IDataExecutionPlanNode)partitionedAggregates;

                    partitionedAggregates.GroupBy.AddRange(GroupBy);

                    foreach (var aggregate in Aggregates)
                    {
                        if (aggregate.Value.AggregateType != AggregateType.Average)
                        {
                            partitionedAggregates.Aggregates[aggregate.Key] = aggregate.Value;
                        }
                        else
                        {
                            // Rewrite AVG as SUM / COUNT
                            partitionedAggregates.Aggregates[aggregate.Key + "_sum"] = new Aggregate
                            {
                                AggregateType = AggregateType.Sum,
                                SqlExpression = aggregate.Value.SqlExpression
                            };
                            partitionedAggregates.Aggregates[aggregate.Key + "_count"] = new Aggregate
                            {
                                AggregateType = AggregateType.Count,
                                SqlExpression = aggregate.Value.SqlExpression
                            };

                            if (partitionedResults == partitionedAggregates)
                            {
                                partitionedResults = new ComputeScalarNode { Source = partitionedAggregates };
                                partitionedAggregates.Parent = partitionedResults;
                            }

                            // Handle count = 0 => null
                            ((ComputeScalarNode)partitionedResults).Columns[aggregate.Key] = new SearchedCaseExpression
                            {
                                WhenClauses =
                                {
                                    new SearchedWhenClause
                                    {
                                        WhenExpression = new BooleanComparisonExpression
                                        {
                                            FirstExpression = (aggregate.Key + "_count").ToColumnReference(),
                                            ComparisonType = BooleanComparisonType.Equals,
                                            SecondExpression = new IntegerLiteral { Value = "0" }
                                        },
                                        ThenExpression = new NullLiteral()
                                    }
                                },
                                ElseExpression = new BinaryExpression
                                {
                                    FirstExpression = (aggregate.Key + "_sum").ToColumnReference(),
                                    BinaryExpressionType = BinaryExpressionType.Divide,
                                    SecondExpression = (aggregate.Key + "_count").ToColumnReference()
                                }
                            };

                            // Find the AVG expression in the FetchXML and replace with _sum and _count
                            var avg = partitionedFetchXml.Entity.FindAliasedAttribute(aggregate.Key, null, out var linkEntity);
                            var sumCount = new object[]
                            {
                                new FetchAttributeType
                                {
                                    name = avg.name,
                                    alias = avg.alias + "_sum",
                                    aggregateSpecified = true,
                                    aggregate = FetchXml.AggregateType.sum
                                },
                                new FetchAttributeType
                                {
                                    name = avg.name,
                                    alias = avg.alias + "_count",
                                    aggregateSpecified = true,
                                    aggregate = FetchXml.AggregateType.countcolumn
                                }
                            };

                            if (linkEntity == null)
                            {
                                partitionedFetchXml.Entity.Items = partitionedFetchXml.Entity.Items
                                    .Except(new[] { avg })
                                    .Concat(sumCount)
                                    .ToArray();
                            }
                            else
                            {
                                linkEntity.Items = linkEntity.Items
                                    .Except(new[] { avg })
                                    .Concat(sumCount)
                                    .ToArray();
                            }
                        }
                    }

                    var tryPartitioned = new TryCatchNode
                    {
                        TrySource = firstTry,
                        CatchSource = partitionedResults,
                        ExceptionFilter = ex => GetOrganizationServiceFault(ex, out var fault) && IsAggregateQueryLimitExceeded(fault)
                    };
                    partitionedResults.Parent = tryPartitioned;
                    firstTry.Parent = tryPartitioned;
                    firstTry = tryPartitioned;
                }

                var tryCatch = new TryCatchNode
                {
                    TrySource = firstTry,
                    CatchSource = this,
                    ExceptionFilter = ex => (ex is QueryExecutionException qee && qee.InnerException is PartitionedAggregateNode.PartitionOverflowException) || (GetOrganizationServiceFault(ex, out var fault) && IsAggregateQueryRetryable(fault))
                };

                firstTry.Parent = tryCatch;
                Parent = tryCatch;
                return tryCatch;
            }

            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            // Columns required by previous nodes must be derived from this node, so no need to pass them through.
            // Just calculate the columns that are required to calculate the groups & aggregates
            var scalarRequiredColumns = new List<string>();
            if (GroupBy != null)
                scalarRequiredColumns.AddRange(GroupBy.Select(g => g.GetColumnName()));

            scalarRequiredColumns.AddRange(Aggregates.Where(agg => agg.Value.SqlExpression != null).SelectMany(agg => agg.Value.SqlExpression.GetColumns()).Distinct());

            Source.AddRequiredColumns(dataSources, parameterTypes, scalarRequiredColumns);
        }
    }
}
