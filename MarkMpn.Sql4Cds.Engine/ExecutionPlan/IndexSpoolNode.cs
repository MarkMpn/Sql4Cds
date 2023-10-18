using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

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
            requiredColumns.Add(KeyColumn);

            Source.AddRequiredColumns(context, requiredColumns);
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            var rows = Source.EstimateRowsOut(context);

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

            return this;
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
