using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Xrm.Sdk.Query;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    public class FakeXrmDataSource : DataSource
    {
        class Reset : IDisposable
        {
            private readonly FakeXrmDataSource _target;
            private readonly Action<FakeXrmDataSource> _reset;

            public Reset(FakeXrmDataSource target, Action<FakeXrmDataSource> set, Action<FakeXrmDataSource> reset)
            {
                _target = target;
                _reset = reset;
                set(target);
            }

            public void Dispose()
            {
                _reset(_target);
            }
        }

        private bool _columnComparisonAvailable = true;
        private bool _orderByEntityNameAvailable = false;
        private bool _useRawOrderByReliable = false;
        private List<JoinOperator> _joinOperators;

        public FakeXrmDataSource()
        {
            _joinOperators = new List<JoinOperator> { JoinOperator.Inner, JoinOperator.LeftOuter };
        }

        public override bool ColumnComparisonAvailable => _columnComparisonAvailable;

        public override bool OrderByEntityNameAvailable => _orderByEntityNameAvailable;

        public override List<JoinOperator> JoinOperatorsAvailable => _joinOperators;

        public override bool UseRawOrderByReliable => _useRawOrderByReliable;

        public IDisposable SetColumnComparison(bool enable)
        {
            var original = _columnComparisonAvailable;
            return new Reset(this, x => x._columnComparisonAvailable = enable, x => x._columnComparisonAvailable = original);
        }

        public IDisposable SetOrderByEntityName(bool enable)
        {
            var original = _orderByEntityNameAvailable;
            return new Reset(this, x => x._orderByEntityNameAvailable = enable, x => x._orderByEntityNameAvailable = original);
        }

        public IDisposable SetUseRawOrderByReliable(bool enable)
        {
            var original = _useRawOrderByReliable;
            return new Reset(this, x => x._useRawOrderByReliable = enable, x => x._useRawOrderByReliable = original);
        }

        public IDisposable EnableJoinOperator(JoinOperator op)
        {
            var add = !JoinOperatorsAvailable.Contains(op);
            return new Reset(this,
                x =>
                {
                    if (add)
                        x.JoinOperatorsAvailable.Add(op);
                },
                x =>
                {
                    if (add)
                        x.JoinOperatorsAvailable.Remove(op);
                });
        }
    }
}
