using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
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

        private CachedList<Entity> _cache;

        /// <summary>
        /// The data source to cache
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNode Source { get; set; }

        protected override IEnumerable<Entity> ExecuteInternal(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes, IDictionary<string, object> parameterValues)
        {
            if (_cache == null)
                _cache = new CachedList<Entity>(Source.Execute(dataSources, options, parameterTypes, parameterValues));

            return _cache;
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        public override NodeSchema GetSchema(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes)
        {
            return Source.GetSchema(dataSources, parameterTypes);
        }

        public override IDataExecutionPlanNode FoldQuery(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            Source = Source.FoldQuery(dataSources, options, parameterTypes);
            Source.Parent = this;
            return this;
        }

        public override void AddRequiredColumns(IDictionary<string, DataSource> dataSources, IDictionary<string, Type> parameterTypes, IList<string> requiredColumns)
        {
            Source.AddRequiredColumns(dataSources, parameterTypes, requiredColumns);
        }

        public override int EstimateRowsOut(IDictionary<string, DataSource> dataSources, IQueryExecutionOptions options, IDictionary<string, Type> parameterTypes)
        {
            return Source.EstimateRowsOut(dataSources, options, parameterTypes);
        }

        public override string ToString()
        {
            return "Table Spool\r\n(Lazy Spool)";
        }
    }
}
