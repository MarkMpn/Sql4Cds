using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    /// <summary>
    /// Provides a rewindable cache of a data source
    /// </summary>
    class TableSpoolNode : BaseDataNode, ISingleSourceExecutionPlanNode
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

        internal int GetCount(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, CancellationToken cancellationToken)
        {
            if (_eagerSpool == null)
                _eagerSpool = Source.Execute(dataSources, options, parameterTypes, parameterValues, cancellationToken).ToArray();

            return _eagerSpool.Length;
        }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IDictionary<string, object> parameterValues, CancellationToken cancellationToken)
        {
            if (SpoolType == SpoolType.Eager)
            {
                if (_eagerSpool == null)
                    _eagerSpool = Source.Execute(dataSources, options, parameterTypes, parameterValues, cancellationToken).ToArray();

                return _eagerSpool;
            }
            else
            {
                if (_lazyCache == null)
                    _lazyCache = new CachedList<Entity>(Source.Execute(dataSources, options, parameterTypes, parameterValues, cancellationToken));

                return _lazyCache;
            }
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override INodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return Source.GetSchema(dataSources, parameterTypes);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes, IList<OptimizerHint> hints)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes, hints);
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, DataTypeReference> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, DataTypeReference> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }

        public override string ToString()
        {
            return $"Table Spool\r\n({SpoolType} Spool)";
        }

        public override object Clone()
        {
            var clone = new TableSpoolNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                SpoolType = SpoolType
            };

            clone.Source.Parent = clone;
            return clone;
        }
    }

    enum SpoolType
    {
        Eager,
        Lazy
    }
}
