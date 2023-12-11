using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
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

        private Entity[] _eagerSpool;
        private CachedList<Entity> _lazyCache;

        /// <summary>
        /// The data source to cache
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        [Category("Table Spool")]
        [DisplayName("Spool Type")]
        public SpoolType SpoolType { get; set; }

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
            if (_eagerSpool == null)
                _eagerSpool = Source.Execute(context).ToArray();

            return _eagerSpool.Length;
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            if (Producer != null)
                return Producer.GetWorkTable();

            if (SpoolType == SpoolType.Eager)
            {
                if (_eagerSpool == null)
                    _eagerSpool = Source.Execute(context).ToArray();

                return _eagerSpool;
            }
            else
            {
                if (_lazyCache == null)
                    _lazyCache = new CachedList<Entity>(Source.Execute(context));

                return _lazyCache;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            if (Source != null)
                yield return Source;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return (Source ?? Producer).GetSchema(context);
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
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            if (Source == null)
                return new RowCountEstimate(1);

            return Source.EstimateRowsOut(context);
        }

        public IEnumerable<Entity> GetWorkTable()
        {
            return (IEnumerable<Entity>)_eagerSpool ?? _lazyCache;
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
                SpoolType = SpoolType
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
