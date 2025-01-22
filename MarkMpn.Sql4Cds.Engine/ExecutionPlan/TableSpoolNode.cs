using System;
using System.Collections;
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
    /// Provides a rewindable cache of a data source
    /// </summary>
    class TableSpoolNode : BaseDataNode, ISingleSourceExecutionPlanNode, ISpoolProducerNode
    {
        class CachedList<T> : IEnumerable<T>
        {
            private readonly IEnumerator<T> _source;
            private readonly List<T> _cache;
            private bool _eof;

            class CachedEnumerator : IEnumerator<T>
            {
                private readonly CachedList<T> _source;
                private int _index;

                public CachedEnumerator(CachedList<T> source)
                {
                    _source = source;
                    _index = -1;
                }

                public T Current => _source._cache[_index];

                object IEnumerator.Current => this.Current;

                public bool MoveNext()
                {
                    _index++;

                    if (_index < _source._cache.Count)
                        return true;

                    if (_source._eof)
                        return false;

                    if (!_source._source.MoveNext())
                    {
                        _source._eof = true;
                        return false;
                    }

                    _source._cache.Add(_source._source.Current);
                    return true;
                }

                public void Reset()
                {
                    _index = -1;
                }

                public void Dispose()
                {
                }
            }

            public CachedList(IEnumerable<T> source)
            {
                _source = source.GetEnumerator();
                _cache = new List<T>();
            }

            public IEnumerator<T> GetEnumerator()
            {
                return new CachedEnumerator(this);
            }

            IEnumerator IEnumerable.GetEnumerator()
            {
                return this.GetEnumerator();
            }
        }

        public TableSpoolNode() { }

        private IEnumerable<Entity> _workTable;

        /// <summary>
        /// The data source to cache
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Category("Table Spool")]
        [DisplayName("Spool Type")]
        public SpoolType SpoolType { get; set; }

        /// <summary>
        /// The segment column to use for partitioning the data
        /// </summary>
        /// <remarks>
        /// If this is set, the node will spool all the data from a single segment and output a single empty row per segment.
        /// </remarks>
        [Category("Table Spool")]
        [DisplayName("Segment Column")]
        [Description("The segment column to use for partitioning the data")]
        public string SegmentColumn { get; set; }

        /// <summary>
        /// Indicates if this spool is in place only for performance reasons
        /// </summary>
        internal bool IsPerformanceSpool { get; set; }

        /// <summary>
        /// The node that produces data that this node should repeat
        /// </summary>
        /// <remarks>
        /// If this property is set, the node operates in Consumer mode.
        /// </remarks>
        [Browsable(false)]
        public ISpoolProducerNode Producer { get; set; }

        [Browsable(false)]
        public ISpoolProducerNode LastClone { get; private set; }

        internal int GetCount(NodeExecutionContext context)
        {
            if (_workTable == null)
                _workTable = Source.Execute(context).ToArray();

            return ((Entity[])_workTable).Length;
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            if (Producer != null)
            {
                foreach (var entity in Producer.GetWorkTable())
                    yield return entity;
            }
            else if (SpoolType == SpoolType.Eager)
            {
                if (_workTable == null)
                    _workTable = Source.Execute(context).ToArray();

                foreach (var entity in _workTable)
                    yield return entity;
            }
            else if (SegmentColumn == null)
            {
                if (_workTable == null)
                    _workTable = new CachedList<Entity>(Source.Execute(context));

                foreach (var entity in _workTable)
                    yield return entity;
            }
            else
            {
                foreach (var entity in Source.Execute(context))
                {
                    var isSegmentStart = ((SqlBoolean)entity[SegmentColumn]).Value;

                    if (isSegmentStart && _workTable != null)
                    {
                        // We've spooled all the data for the previous segment, so output an empty row
                        yield return new Entity();
                    }

                    if (isSegmentStart)
                    {
                        // We're at the start of a new segment, so clear the work table
                        _workTable = new List<Entity>();
                    }

                    // Add this row to the work table
                    ((List<Entity>)_workTable).Add(entity);
                }

                if (_workTable != null)
                {
                    // Output an empty row for the last segment
                    yield return new Entity();
                }
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (Source != null)
                yield return Source;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            if (SegmentColumn != null)
            {
                // When being used for a window aggregate, we only need to output an empty row for each segment
                // rather than the actual data
                return new NodeSchema(null, null, null, null);
            }

            if (Source != null)
                return Source.GetSchema(context);

            return Producer.GetWorkTableSchema(context);
        }

        public INodeSchema GetWorkTableSchema(NodeCompilationContext context)
        {
            return Source.GetSchema(context);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            if (Source == null)
                return this;

            Source = Source.FoldQuery(context, hints);

            if (IsPerformanceSpool && hints != null && hints.Any(hint => hint.HintKind == OptimizerHintKind.NoPerformanceSpool))
                return Source;

            if (Source is ConstantScanNode)
                return Source;

            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            Source?.AddRequiredColumns(context, requiredColumns);

            if (Producer != null && !IsSourceOf(Producer))
                Producer.AddRequiredColumns(context, requiredColumns);
        }

        private bool IsSourceOf(IExecutionPlanNode producer)
        {
            return IsSourceOf(this, producer);
        }

        private static bool IsSourceOf(IExecutionPlanNode node, IExecutionPlanNode producer)
        {
            if (node == producer)
                return true;

            foreach (var source in producer.GetSources())
            {
                if (IsSourceOf(node, source))
                    return true;
            }

            return false;
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            if (Source == null)
                return new RowCountEstimate(1);

            return Source.EstimateRowsOut(context);
        }

        public IEnumerable<Entity> GetWorkTable()
        {
            return _workTable;
        }

        public override string ToString()
        {
            return $"Table Spool\r\n({SpoolType} Spool)";
        }

        public override object Clone()
        {
            var clone = new TableSpoolNode
            {
                Producer = Producer?.LastClone,
                SpoolType = SpoolType,
                SegmentColumn = SegmentColumn,
            };

            LastClone = clone;

            if (Source != null)
            {
                clone.Source = (IDataExecutionPlanNodeInternal)Source.Clone();
                clone.Source.Parent = clone;
            }

            return clone;
        }
    }

    enum SpoolType
    {
        Eager,
        Lazy
    }
}
