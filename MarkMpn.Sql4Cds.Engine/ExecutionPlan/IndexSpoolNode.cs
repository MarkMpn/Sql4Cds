using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.FetchXml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;
using Microsoft.Xrm.Sdk.Metadata;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Stores data in a hashtable for fast lookups
    /// </summary>
    class IndexSpoolNode : BaseDataNode, ISingleSourceExecutionPlanNode, ISpoolProducerNode
    {
        private IDictionary<INullable, List<Entity>> _hashTable;
        private Func<INullable, INullable> _keySelector;
        private Func<INullable, INullable> _seekSelector;
        private Stack<Entity> _stack;

        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        /// <summary>
        /// The column in the data source to create an index on
        /// </summary>
        [Category("Index Spool")]
        [DisplayName("Key Column")]
        [Description("The column in the data source to create an index on")]
        public string KeyColumn { get; set; }

        /// <summary>
        /// The name of the parameter to use for seeking in the index
        /// </summary>
        [Category("Index Spool")]
        [DisplayName("Seek Value")]
        [Description("The name of the parameter to use for seeking in the index")]
        public string SeekValue { get; set; }

        /// <summary>
        /// Stores the records in a stack to support recursive CTEs
        /// </summary>
        [Category("Index Spool")]
        [DisplayName("With Stack")]
        [Description("Stores the records in a stack to support recursive CTEs")]
        public bool WithStack { get; set; }

        [Browsable(false)]
        public ISpoolProducerNode LastClone { get; private set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            if (KeyColumn != null)
                requiredColumns.Add(KeyColumn);

            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var rows = Source.EstimateRowsOut(context);

            if (KeyColumn == null)
                return rows;

            if (rows is RowCountEstimateDefiniteRange range && range.Maximum == 1)
                return range;

            return new RowCountEstimate(Source.EstimatedRowsOut / 100);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(context, hints);

            if (KeyColumn != null && SeekValue != null)
            {
                // Index and seek values must be the same type
                var indexType = Source.GetSchema(context).Schema[KeyColumn].Type;
                var seekType = context.ParameterTypes[SeekValue];

                if (!SqlTypeConverter.CanMakeConsistentTypes(indexType, seekType, context.PrimaryDataSource, out var consistentType))
                    throw new QueryExecutionException($"No type conversion available for {indexType.ToSql()} and {seekType.ToSql()}");

                _keySelector = SqlTypeConverter.GetConversion(indexType, consistentType);
                _seekSelector = SqlTypeConverter.GetConversion(seekType, consistentType);
            }

            if (WithStack)
                return FoldCTEToFetchXml(context, hints);

            return this;
        }

        private IDataExecutionPlanNodeInternal FoldCTEToFetchXml(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // We can use above/below FetchXML conditions for common CTE patterns
            // https://learn.microsoft.com/en-us/power-apps/developer/data-platform/query-hierarchical-data
            // This always uses the default max recursion depth of 100, so don't use it if we have any other hint
            var maxRecursion = hints
                .OfType<LiteralOptimizerHint>()
                .Where(hint => hint.HintKind == OptimizerHintKind.MaxRecursion)
                .FirstOrDefault()
                ?.Value
                ?.Value
                ?? "100";

            if (maxRecursion != "100")
                return this;

            // Check we have the required execution plan pattern:
            //
            // Index Spool ━━ Concatenate ━━ Compute Scalar ━━ FetchXML Query
            //                            ┕ Assert ━━ Nested Loop ━━ Compute Scalar ━━ Table Spool
            //                                                    ┕ Index Spool ━━ FetchXML Query
            //                                                                  ┕ FetchXML Query

            var concat = Source as ConcatenateNode;
            if (concat == null || concat.Sources.Count != 2)
                return this;

            var initialDepthCompute = concat.Sources[0] as ComputeScalarNode;
            if (initialDepthCompute == null)
                return this;

            var anchorFetchXml = initialDepthCompute.Source as FetchXmlScan;
            if (anchorFetchXml == null)
                return this;

            var depthAssert = concat.Sources[1] as AssertNode;
            if (depthAssert == null)
                return this;

            var recurseLoop = depthAssert.Source as NestedLoopNode;
            if (recurseLoop == null)
                return this;

            var incrementDepthCompute = recurseLoop.LeftSource as ComputeScalarNode;
            if (incrementDepthCompute == null)
                return this;

            var recurseSpoolConsumer = incrementDepthCompute.Source as TableSpoolNode;
            if (recurseSpoolConsumer == null || recurseSpoolConsumer.Source != null)
                return this;

            var adaptiveSpool = recurseLoop.RightSource as AdaptiveIndexSpoolNode;
            if (adaptiveSpool == null)
                return this;

            var unspooledRecursiveFetchXml = adaptiveSpool.UnspooledSource as FetchXmlScan;
            if (unspooledRecursiveFetchXml == null)
                return this;

            var spooledRecursiveFetchXml = adaptiveSpool.SpooledSource as FetchXmlScan;
            if (spooledRecursiveFetchXml == null)
                return this;

            // We can only use the hierarchical FetchXML filters if the recursion is within the same entity and is using only the
            // hierarchical relationship for filtering
            if (anchorFetchXml.DataSource != spooledRecursiveFetchXml.DataSource ||
                anchorFetchXml.Entity.name != spooledRecursiveFetchXml.Entity.name)
                return this;

            // Check for any other filters or link-entities
            if (spooledRecursiveFetchXml.Entity.GetLinkEntities().Any() ||
                spooledRecursiveFetchXml.Entity.Items != null && spooledRecursiveFetchXml.Entity.Items.OfType<filter>().Any())
                return this;

            // Check there are no extra calculated columns
            if (initialDepthCompute.Columns.Count != 1 || incrementDepthCompute.Columns.Count != 1)
                return this;

            // Check all columns are consistent
            var depthField = initialDepthCompute.Columns.Single().Key;

            for (var i = 0; i < concat.ColumnSet.Count; i++)
            {
                if (concat.ColumnSet[i].SourceColumns[0] == depthField)
                    continue;

                var anchorAttribute = concat.ColumnSet[i].SourceColumns[0];
                var recurseAttribute = concat.ColumnSet[i].SourceColumns[1];
                recurseAttribute = recurseLoop.DefinedValues[recurseAttribute];

                // Ignore any differences in the aliases used for the anchor and recursive parts
                anchorAttribute = anchorAttribute.ToColumnReference().MultiPartIdentifier.Identifiers.Last().Value;
                recurseAttribute = recurseAttribute.ToColumnReference().MultiPartIdentifier.Identifiers.Last().Value;

                if (anchorAttribute != recurseAttribute)
                    return this;
            }

            var metadata = context.DataSources[anchorFetchXml.DataSource].Metadata[anchorFetchXml.Entity.name];
            var hierarchicalRelationship = metadata.OneToManyRelationships.SingleOrDefault(r => r.IsHierarchical == true);

            if (hierarchicalRelationship == null ||
                hierarchicalRelationship.ReferencingEntity != hierarchicalRelationship.ReferencedEntity)
                return this;

            var anchorKey = adaptiveSpool.SeekValue; // Will be the variable name defined by the recursion loop
            anchorKey = recurseLoop.OuterReferences.Single(kvp => kvp.Value == anchorKey).Key; // Will now be the column name defined by the concatenate node
            anchorKey = concat.ColumnSet.Single(col => col.OutputColumn == anchorKey).SourceColumns[0]; // Will now be the column from the anchor FetchXML
            var anchorCol = anchorKey.ToColumnReference();
            if (anchorCol.MultiPartIdentifier.Count != 2 ||
                anchorCol.MultiPartIdentifier.Identifiers[0].Value != anchorFetchXml.Alias)
                return this;
            var anchorAttr = anchorCol.MultiPartIdentifier[1].Value;

            var recurseCol = adaptiveSpool.KeyColumn.ToColumnReference();
            if (recurseCol.MultiPartIdentifier.Count != 2 ||
                recurseCol.MultiPartIdentifier.Identifiers[0].Value != spooledRecursiveFetchXml.Alias)
                return this;
            var recurseAttr = recurseCol.MultiPartIdentifier[1].Value;

            var isUnder = anchorAttr == hierarchicalRelationship.ReferencedAttribute && recurseAttr == hierarchicalRelationship.ReferencingAttribute;
            var isAbove = anchorAttr == hierarchicalRelationship.ReferencingAttribute && recurseAttr == hierarchicalRelationship.ReferencedAttribute;

            if (!isUnder && !isAbove)
                return this;

            // The depth counter is no longer generated or used, so remove it from the concat column list
            var depthFieldConcatColumn = concat.ColumnSet.Single(c => c.SourceColumns[0] == depthField);
            concat.ColumnSet.Remove(depthFieldConcatColumn);

            // We can replace the whole CTE with a single eq-or-above or eq-or-under FetchXML if the anchor
            // query filters on a single primary key
            var at = GetPrimaryKeyFilter(anchorFetchXml, metadata);

            if (at != null)
            {
                at.@operator = isUnder ? @operator.eqorunder : @operator.eqorabove;

                // We might have some column renamings applied, so update them too
                var alias = Parent as AliasNode;

                if (alias == null)
                {
                    foreach (var col in concat.ColumnSet)
                        anchorFetchXml.ColumnMappings.Add(new SelectColumn { SourceColumn = col.SourceColumns[0], OutputColumn = col.OutputColumn });
                }
                else
                {
                    foreach (var col in alias.ColumnSet)
                    {
                        var concatCol = concat.ColumnSet.Single(c => c.OutputColumn == col.SourceColumn);
                        col.SourceColumn = concatCol.SourceColumns[0];
                    }
                }

                return anchorFetchXml;
            }

            // We can replace the recursive part with a nested loop calling an above or under FetchXML if the anchor
            // query is a more complex filter. We don't want to recurse into the results of this second FetchXML though as the recursion
            // has already happened server-side, so the execution plan should become:
            //
            // Concatenate ━━ Index Spool ━━ FetchXML Query
            //             ┕ Nested Loop ━━ Table Spool
            //                           ┕ FetchXML Query

            var recurseCondition = (condition) unspooledRecursiveFetchXml.Entity.Items.OfType<filter>().Single().Items.Single();
            recurseCondition.attribute = metadata.PrimaryIdAttribute;
            recurseCondition.@operator = isUnder ? @operator.under : @operator.above;

            concat.Sources[0] = this;
            Parent = concat;
            Source = anchorFetchXml;
            anchorFetchXml.Parent = this;
            concat.Sources[1] = recurseLoop;
            recurseLoop.Parent = concat;
            recurseLoop.LeftSource = recurseSpoolConsumer;
            recurseSpoolConsumer.Parent = recurseLoop;
            recurseLoop.RightSource = unspooledRecursiveFetchXml;
            unspooledRecursiveFetchXml.Parent = recurseLoop;

            // The spooled data will now be using the original names from the anchor FetchXML node rather than the renamed
            // versions from the Concatenate node, so rewrite the outer references
            var outerReferences = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var outerRef in recurseLoop.OuterReferences)
            {
                var concatCol = concat.ColumnSet.Single(c => c.OutputColumn == outerRef.Key);
                outerReferences[concatCol.SourceColumns[0]] = outerRef.Value;
            }

            recurseLoop.OuterReferences = outerReferences;

            return concat;
        }

        private condition GetPrimaryKeyFilter(FetchXmlScan anchorFetchXml, EntityMetadata metadata)
        {
            if (anchorFetchXml.Entity.Items == null)
                return null;
            var anchorFilters = anchorFetchXml.Entity.Items.OfType<filter>().ToArray();
            if (anchorFilters.Length != 1)
                return null;
            if (anchorFilters[0].Items == null || anchorFilters[0].Items.Length != 1 || !(anchorFilters[0].Items[0] is condition anchorCondition))
                return null;
            if (anchorCondition.attribute != metadata.PrimaryIdAttribute || anchorCondition.@operator != @operator.eq)
                return null;

            return anchorCondition;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return Source.GetSchema(context);
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            if (WithStack)
                return ExecuteInternalWithStack(context);

            // Build an internal hash table of the source indexed by the key column
            if (_hashTable == null)
            {
                _hashTable = Source.Execute(context)
                    .GroupBy(e => _keySelector((INullable)e[KeyColumn]))
                    .ToDictionary(g => g.Key, g => g.ToList());
            }

            var keyValue = _seekSelector((INullable)context.ParameterValues[SeekValue]);

            if (!_hashTable.TryGetValue(keyValue, out var matches))
                return Array.Empty<Entity>();

            return matches;
        }

        private IEnumerable<Entity> ExecuteInternalWithStack(NodeExecutionContext context)
        {
            _stack = new Stack<Entity>();

            foreach (var entity in Source.Execute(context))
            {
                _stack.Push(entity);
                yield return entity;
            }
        }

        public IEnumerable<Entity> GetWorkTable()
        {
            while (_stack.Count > 0)
                yield return _stack.Pop();
        }

        public override string ToString()
        {
            if (WithStack)
                return "Index Spool\r\n(Lazy Spool)";

            return "Index Spool\r\n(Eager Spool)";
        }

        public override object Clone()
        {
            var clone = new IndexSpoolNode
            {
                KeyColumn = KeyColumn,
                SeekValue = SeekValue,
                _keySelector = _keySelector,
                _seekSelector = _seekSelector,
                WithStack = WithStack,
            };

            LastClone = clone;

            clone.Source = (IDataExecutionPlanNodeInternal)Source.Clone();
            clone.Source.Parent = clone;

            return clone;
        }
    }
}
