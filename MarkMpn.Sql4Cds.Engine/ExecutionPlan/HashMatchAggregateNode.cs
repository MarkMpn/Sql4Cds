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

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var schema = Source.GetSchema(context);
            var groupByCols = GetGroupingColumns(schema);
            var groups = new Dictionary<Entity, Dictionary<string, AggregateFunctionState>>(new DistinctEqualityComparer(groupByCols));

            InitializeAggregates(new ExpressionCompilationContext(context, schema, null));
            var executionContext = new ExpressionExecutionContext(context);
            var aggregates = CreateAggregateFunctions(executionContext, false);

            if (IsScalarAggregate)
            {
                // Initialize the single group
                var values = ResetAggregates(aggregates);
                groups[new Entity()] = values;
            }

            foreach (var entity in Source.Execute(context))
            {
                if (!groups.TryGetValue(entity, out var values))
                {
                    values = ResetAggregates(aggregates);
                    groups[entity] = values;
                }

                executionContext.Entity = entity;

                foreach (var func in values.Values)
                    func.AggregateFunction.NextRecord(func.State);
            }

            foreach (var group in groups)
            {
                var result = new Entity();

                for (var i = 0; i < groupByCols.Count; i++)
                    result[groupByCols[i]] = group.Key[groupByCols[i]];

                foreach (var aggregate in GetValues(group.Value))
                    result[aggregate.Key] = aggregate.Value;

                yield return result;
            }
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (_folded)
                return this;

            // FoldQuery can be called again in some circumstances. Don't repeat the folding operation and create another try/catch
            _folded = true;

            Source = Source.FoldQuery(context, hints);
            Source.Parent = this;

            // Special case for using RetrieveTotalRecordCount instead of FetchXML
            var retrieveTotalRecordCount = hints != null && hints
                .OfType<UseHintList>()
                .Where(useHint => useHint.Hints.Any(hint => hint.Value.Equals("RETRIEVE_TOTAL_RECORD_COUNT", StringComparison.OrdinalIgnoreCase)))
                .Any();

            if (retrieveTotalRecordCount &&
                Source is FetchXmlScan fetch &&
                (fetch.Entity.Items == null || fetch.Entity.Items.Length == 0) &&
                GroupBy.Count == 0 &&
                Aggregates.Count == 1 &&
                Aggregates.Single().Value.AggregateType == AggregateType.CountStar &&
                context.DataSources[fetch.DataSource].Metadata[fetch.Entity.name].DataProviderId == null && // RetrieveTotalRecordCountRequest is not valid for virtual entities
                fetch.FetchXml.DataSource == null) // RetrieveTotalRecordCountRequest is not valid for archive data
            {
                var count = new RetrieveTotalRecordCountNode { DataSource = fetch.DataSource, EntityName = fetch.Entity.name };
                var countName = count.GetSchema(context).Schema.Single().Key;

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
                // be handled - if any one needs to be calculated manually, we need to calculate them all.
                var canUseFetchXmlAggregate = true;

                // Also track if we can partition the query for larger source data sets. We can't partition DISTINCT aggregates,
                // and need to transform AVG(field) to SUM(field) / COUNT(field)
                var canPartition = true;

                foreach (var agg in Aggregates)
                {
                    if (agg.Value.SqlExpression != null && !(agg.Value.SqlExpression is ColumnReferenceExpression))
                    {
                        canUseFetchXmlAggregate = false;
                        break;
                    }

                    if (agg.Value.Distinct && agg.Value.AggregateType != AggregateType.Count)
                    {
                        canUseFetchXmlAggregate = false;
                        break;
                    }

                    if (agg.Value.AggregateType == AggregateType.First || agg.Value.AggregateType == AggregateType.StringAgg)
                    {
                        canUseFetchXmlAggregate = false;
                        break;
                    }

                    if (agg.Value.Distinct)
                        canPartition = false;

                    if (agg.Value.AggregateType == AggregateType.StringAgg)
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
                        if (!(scalar.Value is FunctionCall func))
                        {
                            canUseFetchXmlAggregate = false;
                            break;
                        }

                        string datePartType;

                        if (!func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase) &&
                            !func.FunctionName.Value.Equals("YEAR", StringComparison.OrdinalIgnoreCase) &&
                            !func.FunctionName.Value.Equals("MONTH", StringComparison.OrdinalIgnoreCase) &&
                            !func.FunctionName.Value.Equals("DAY", StringComparison.OrdinalIgnoreCase))
                        {
                            canUseFetchXmlAggregate = false;
                            break;
                        }

                        if (func.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase))
                        {
                            if (func.Parameters.Count != 2 ||
                                !(func.Parameters[0] is ColumnReferenceExpression datePartTypeCol) ||
                                !(func.Parameters[1] is ColumnReferenceExpression))
                            {
                                canUseFetchXmlAggregate = false;
                                break;
                            }
                            else
                            {
                                datePartType = datePartTypeCol.GetColumnName();
                            }
                        }
                        else
                        {
                            if (func.Parameters.Count != 1 ||
                                !(func.Parameters[0] is ColumnReferenceExpression))
                            {
                                canUseFetchXmlAggregate = false;
                                break;
                            }
                            else
                            {
                                datePartType = func.FunctionName.Value;
                            }
                        }

                        if (!GroupBy.Any(g => g.MultiPartIdentifier.Identifiers.Count == 1 && g.MultiPartIdentifier.Identifiers[0].Value == scalar.Key))
                        {
                            canUseFetchXmlAggregate = false;
                            break;
                        }

                        if (!partnames.ContainsKey(datePartType))
                        {
                            canUseFetchXmlAggregate = false;
                            break;
                        }

                        // FetchXML dategrouping always uses local timezone. If we're using UTC we can't use it
                        if (!context.Options.UseLocalTimeZone)
                        {
                            canUseFetchXmlAggregate = false;
                            break;
                        }
                    }
                }

                var metadata = context.DataSources[fetchXml.DataSource].Metadata;

                // Aggregates are not supported on archive data
                if (fetchXml.FetchXml.DataSource != null)
                    canUseFetchXmlAggregate = false;

                // FetchXML is translated to QueryExpression for virtual entities, which doesn't support aggregates
                // Elastic tables do support aggregates though.
                if (metadata[fetchXml.Entity.name].DataProviderId != null &&
                    metadata[fetchXml.Entity.name].DataProviderId != DataProviders.ElasticDataProvider)
                    canUseFetchXmlAggregate = false;

                // Check FetchXML supports grouping by each of the requested attributes
                var fetchSchema = fetchXml.GetSchema(context);
                var maxResultCount = 0;

                foreach (var group in GroupBy)
                {
                    if (!fetchSchema.ContainsColumn(group.GetColumnName(), out var groupCol))
                    {
                        // Grouping by a DATEPART calculation from the CalculateScalar node.
                        maxResultCount = Int32.MaxValue;
                        continue;
                    }

                    var parts = groupCol.SplitMultiPartIdentifier();
                    string entityName;

                    if (parts[0] == fetchXml.Alias)
                        entityName = fetchXml.Entity.name;
                    else
                        entityName = fetchXml.Entity.FindLinkEntity(parts[0]).name;

                    var attr = metadata[entityName].Attributes.SingleOrDefault(a => a.LogicalName == parts[1]);

                    // Can't group by virtual attributes
                    if (attr == null || attr.AttributeOf != null)
                        canUseFetchXmlAggregate = false;

                    // Can't group by multi-select picklist attributes
                    if (attr is MultiSelectPicklistAttributeMetadata)
                        canUseFetchXmlAggregate = false;

                    // Track how many results could be produced by this grouping
                    if (maxResultCount < Int32.MaxValue && attr is EnumAttributeMetadata enumAttr)
                    {
                        if (maxResultCount == 0)
                            maxResultCount = enumAttr.OptionSet.Options.Count;
                        else
                            maxResultCount *= enumAttr.OptionSet.Options.Count;
                    }
                    else
                    {
                        maxResultCount = Int32.MaxValue;
                    }
                }

                // Cosmos DB can't use sorting and grouping together, so we can't use FetchXML aggregates
                // if we'll need to page the results. Applies to the audit entity as well, even though it's
                // not using the elastic table provider
                if (maxResultCount > 500 &&
                    (fetchXml.Entity.name == "audit" || metadata[fetchXml.Entity.name].DataProviderId == DataProviders.ElasticDataProvider))
                    canUseFetchXmlAggregate = false;

                var serializer = new XmlSerializer(typeof(FetchXml.FetchType));

                if (canUseFetchXmlAggregate)
                {
                    // FetchXML aggregates can trigger an AggregateQueryRecordLimitExceeded error. Clone the non-aggregate FetchXML
                    // so we can try to run the native aggregate version but fall back to in-memory processing where necessary
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

                    var schema = Source.GetSchema(context);
                    var expressionCompilationContext = new ExpressionCompilationContext(context, schema, null);

                    foreach (var grouping in GroupBy)
                    {
                        var colName = grouping.GetColumnName();
                        var alias = grouping.MultiPartIdentifier.Identifiers.Last().Value;
                        DateGroupingType? dateGrouping = null;

                        if (computeScalar != null && computeScalar.Columns.TryGetValue(colName, out var datePart))
                        {
                            var datePartFunc = (FunctionCall)datePart;

                            if (datePartFunc.FunctionName.Value.Equals("DATEPART", StringComparison.OrdinalIgnoreCase))
                            {
                                dateGrouping = partnames[((ColumnReferenceExpression)datePartFunc.Parameters[0]).GetColumnName()];
                                colName = ((ColumnReferenceExpression)datePartFunc.Parameters[1]).GetColumnName();
                            }
                            else
                            {
                                dateGrouping = partnames[datePartFunc.FunctionName.Value];
                                colName = ((ColumnReferenceExpression)datePartFunc.Parameters[0]).GetColumnName();
                            }
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
                        else if (grouping.GetType(expressionCompilationContext, out _) == typeof(SqlDateTime))
                        {
                            // Can't group on datetime columns without a DATEPART specification
                            canUseFetchXmlAggregate = false;
                        }

                        // Add a sort order for each grouping to allow consistent paging if required
                        if (maxResultCount > 5000)
                        {
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
                    }

                    foreach (var agg in Aggregates)
                    {
                        var col = (ColumnReferenceExpression)agg.Value.SqlExpression;
                        var colName = col == null ? (fetchXml.Alias + "." + metadata[fetchXml.Entity.name].PrimaryIdAttribute) : col.GetColumnName();

                        if (!schema.ContainsColumn(colName, out colName))
                            canUseFetchXmlAggregate = false;

                        var distinct = agg.Value.Distinct ? FetchBoolType.@true : FetchBoolType.@false;

                        FetchXml.AggregateType aggregateType;

                        switch (agg.Value.AggregateType)
                        {
                            case AggregateType.Average:
                                aggregateType = FetchXml.AggregateType.avg;
                                break;

                            case AggregateType.Count:
                                aggregateType = FetchXml.AggregateType.countcolumn;
                                break;

                            case AggregateType.CountStar:
                                aggregateType = FetchXml.AggregateType.count;
                                break;

                            case AggregateType.Max:
                                aggregateType = FetchXml.AggregateType.max;
                                break;

                            case AggregateType.Min:
                                aggregateType = FetchXml.AggregateType.min;
                                break;

                            case AggregateType.Sum:
                                aggregateType = FetchXml.AggregateType.sum;
                                break;

                            default:
                                throw new ArgumentOutOfRangeException();
                        }

                        // min, max, sum and avg are not supported for optionset & boolean attributes
                        var parts = colName.SplitMultiPartIdentifier();
                        string entityName;

                        if (parts[0] == fetchXml.Alias)
                            entityName = fetchXml.Entity.name;
                        else
                            entityName = fetchXml.Entity.FindLinkEntity(parts[0]).name;

                        var attr = metadata[entityName].Attributes.SingleOrDefault(a => a.LogicalName == parts[1]);

                        if (attr == null)
                            canUseFetchXmlAggregate = false;

                        if ((attr is EnumAttributeMetadata || attr is BooleanAttributeMetadata) && (aggregateType == FetchXml.AggregateType.avg || aggregateType == FetchXml.AggregateType.max || aggregateType == FetchXml.AggregateType.min || aggregateType == FetchXml.AggregateType.sum))
                            canUseFetchXmlAggregate = false;

                        // min and max are not supported for primary key and lookup attributes
                        if ((attr is LookupAttributeMetadata || attr is UniqueIdentifierAttributeMetadata || attr?.IsPrimaryId == true) && (aggregateType == FetchXml.AggregateType.min || aggregateType == FetchXml.AggregateType.max))
                            canUseFetchXmlAggregate = false;

                        var attribute = fetchXml.AddAttribute(colName, a => a.aggregate == aggregateType && a.alias == agg.Key && a.distinct == distinct, metadata, out _, out _);

                        if (attribute.name != attr?.LogicalName)
                        {
                            // We asked for a ___name or ___type virtual attribute, so the underlying attribute was added. This doesn't give the
                            // correct result, so we can't use FetchXML aggregation.
                            canUseFetchXmlAggregate = false;
                        }

                        attribute.aggregate = aggregateType;
                        attribute.aggregateSpecified = true;
                        attribute.alias = agg.Key;

                        if (agg.Value.Distinct)
                        {
                            attribute.distinct = distinct;
                            attribute.distinctSpecified = true;
                        }
                    }
                }

                // Check how we should execute this aggregate if the FetchXML aggregate fails or is not available. Use stream aggregate
                // for scalar aggregates or where all the grouping fields can be folded into sorts.
                var nonFetchXmlAggregate = FoldToStreamAggregate(context, hints);

                if (!canUseFetchXmlAggregate)
                    return nonFetchXmlAggregate;

                IDataExecutionPlanNodeInternal firstTry = fetchXml;

                // If the main aggregate query fails due to having over 50K records, check if we can retry with partitioning. We
                // need a createdon field to be available for this to work.
                if (canPartition)
                    canPartition = metadata[fetchXml.Entity.name].Attributes.Any(a => a.LogicalName == "createdon");

                if (canUseFetchXmlAggregate && canPartition)
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
                    var partitionedResults = (IDataExecutionPlanNodeInternal)partitionedAggregates;

                    partitionedAggregates.GroupBy.AddRange(GroupBy);

                    foreach (var aggregate in Aggregates)
                    {
                        if (aggregate.Value.AggregateType != AggregateType.Average)
                        {
                            partitionedAggregates.Aggregates[aggregate.Key] = new Aggregate
                            {
                                AggregateType = aggregate.Value.AggregateType,
                                Distinct = aggregate.Value.Distinct,
                                SqlExpression = aggregate.Key.ToColumnReference()
                            };
                        }
                        else
                        {
                            // Rewrite AVG as SUM / COUNT
                            partitionedAggregates.Aggregates[aggregate.Key + "_sum"] = new Aggregate
                            {
                                AggregateType = AggregateType.Sum,
                                SqlExpression = (aggregate.Key + "_sum").ToColumnReference()
                            };
                            partitionedAggregates.Aggregates[aggregate.Key + "_count"] = new Aggregate
                            {
                                AggregateType = AggregateType.Count,
                                SqlExpression = (aggregate.Key + "_count").ToColumnReference()
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
                        CatchSource = partitionedResults.FoldQuery(context, hints),
                        ExceptionFilter = ex => GetOrganizationServiceFault(ex, out var fault) && IsAggregateQueryLimitExceeded(fault)
                    };
                    partitionedResults.Parent = tryPartitioned;
                    firstTry.Parent = tryPartitioned;
                    firstTry = tryPartitioned;
                }

                var tryCatch = new TryCatchNode
                {
                    TrySource = firstTry,
                    CatchSource = nonFetchXmlAggregate,
                    ExceptionFilter = ex => (ex is QueryExecutionException qee && (qee.InnerException is PartitionedAggregateNode.PartitionOverflowException || qee.InnerException is FetchXmlScan.InvalidPagingException)) || (GetOrganizationServiceFault(ex, out var fault) && (IsAggregateQueryRetryable(fault) || IsCompositeAddressPluginBug(fault)))
                };

                firstTry.Parent = tryCatch;
                nonFetchXmlAggregate.Parent = tryCatch;
                return tryCatch;
            }

            return FoldToStreamAggregate(context, hints);
        }

        private IDataExecutionPlanNodeInternal FoldToStreamAggregate(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // Use stream aggregate where possible - if there are no grouping fields or the groups can be folded into sorts
            var streamAggregate = new StreamAggregateNode { Source = Source };
            streamAggregate.GroupBy.AddRange(GroupBy);

            foreach (var aggregate in Aggregates)
                streamAggregate.Aggregates[aggregate.Key] = aggregate.Value;

            foreach (var sort in WithinGroupSorts)
                streamAggregate.WithinGroupSorts.Add(sort);

            if (!IsScalarAggregate || WithinGroupSorts.Any())
            {
                // Use hash grouping if explicitly requested with optimizer hint
                if (hints != null && hints.Any(h => h.HintKind == OptimizerHintKind.HashGroup))
                    return FoldWithinGroupSorts(context, hints);

                var sorts = new SortNode { Source = Source };

                foreach (var group in GroupBy)
                    sorts.Sorts.Add(new ExpressionWithSortOrder { Expression = group, SortOrder = SortOrder.Ascending });

                foreach (var sort in WithinGroupSorts)
                    sorts.Sorts.Add(sort);

                streamAggregate.Source = sorts.FoldQuery(context, hints);

                // Don't bother using a sort + stream aggregate if none of the sorts can be folded
                if (streamAggregate.Source == sorts && sorts.PresortedCount == 0)
                    return FoldWithinGroupSorts(context, hints);
            }

            return streamAggregate;
        }

        private IDataExecutionPlanNodeInternal FoldWithinGroupSorts(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (WithinGroupSorts.Count == 0)
                return this;

            // If we have sorts to apply within groups, sort the original data and then apply the aggregate
            var sort = new SortNode { Source = Source };

            foreach (var s in WithinGroupSorts)
                sort.Sorts.Add(s);

            Source = sort.FoldQuery(context, hints);
            return this;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (!_folded)
                return;

            // Columns required by previous nodes must be derived from this node, so no need to pass them through.
            // Just calculate the columns that are required to calculate the groups & aggregates
            var scalarRequiredColumns = new List<string>();
            if (GroupBy != null)
                scalarRequiredColumns.AddRange(GroupBy.Select(g => g.GetColumnName()));

            scalarRequiredColumns.AddRange(Aggregates.Where(agg => agg.Value.SqlExpression != null).SelectMany(agg => agg.Value.SqlExpression.GetColumns()).Distinct());

            Source.AddRequiredColumns(context, scalarRequiredColumns);
        }

        public override object Clone()
        {
            var clone = new HashMatchAggregateNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                _folded = _folded
            };

            foreach (var kvp in Aggregates)
                clone.Aggregates.Add(kvp.Key, kvp.Value);

            clone.GroupBy.AddRange(GroupBy);
            clone.Source.Parent = clone;

            foreach (var sort in WithinGroupSorts)
                clone.WithinGroupSorts.Add(sort.Clone());

            return clone;
        }
    }
}
