using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.Engine
{
    public class Sql4CdsCommand : DbCommand
    {
        private Sql4CdsConnection _connection;
        private string _commandText;
        private IRootExecutionPlanNode[] _plan;
        private CancellationTokenSource _cts;

        public Sql4CdsCommand(Sql4CdsConnection connection) : this(connection, string.Empty)
        {
        }

        public Sql4CdsCommand(Sql4CdsConnection connection, string commandText)
        {
            _connection = connection;
            CommandText = commandText;
            CommandTimeout = 30;
            DbParameterCollection = new Sql4CdsParameterCollection();
        }

        internal IRootExecutionPlanNode[] Plan => _plan;

        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                _commandText = value;
                _plan = null;
            }
        }

        public override int CommandTimeout { get; set; }
        
        public override CommandType CommandType
        {
            get { return CommandType.Text; }
            set
            {
                if (value != CommandType.Text)
                    throw new ArgumentOutOfRangeException("Only CommandType.Text is supported");
            }
        }
        
        public override bool DesignTimeVisible { get; set; }
        
        public override UpdateRowSource UpdatedRowSource { get; set; }

        public event EventHandler<StatementCompletedEventArgs> StatementCompleted;

        internal void OnStatementCompleted(int recordsAffected)
        {
            var handler = StatementCompleted;

            if (handler != null)
                handler(this, new StatementCompletedEventArgs(recordsAffected));
        }
        
        protected override DbConnection DbConnection
        {
            get { return _connection; }
            set
            {
                if (value == null)
                    throw new ArgumentOutOfRangeException(nameof(value), "Connection must be specified");

                if (!(_connection is Sql4CdsConnection con))
                    throw new ArgumentOutOfRangeException(nameof(value), "Connection must be a Sql4CdsConnection");

                _connection = con;
                _plan = null;
            }
        }

        protected override DbParameterCollection DbParameterCollection { get; }

        protected override DbTransaction DbTransaction
        {
            get { return null; }
            set
            {
                if (value != null)
                    throw new ArgumentOutOfRangeException("Transactions are not supported");
            }
        }

        public override void Cancel()
        {
            _cts?.Cancel();
        }

        public override int ExecuteNonQuery()
        {
            using (var reader = ExecuteReader())
            {
                return reader.RecordsAffected;
            }
        }

        public override object ExecuteScalar()
        {
            using (var reader = ExecuteReader())
            {
                if (!reader.Read())
                    return null;

                return reader.GetValue(0);
            }    
        }

        public override void Prepare()
        {
            if (_plan != null)
                return;

            _plan = _connection.PlanBuilder.Build(CommandText, ((Sql4CdsParameterCollection)Parameters).GetParameterTypes());
        }

        protected override DbParameter CreateDbParameter()
        {
            return new Sql4CdsParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            Prepare();

            _cts = CommandTimeout == 0 ? new CancellationTokenSource() : new CancellationTokenSource(TimeSpan.FromSeconds(CommandTimeout));
            var options = new CancellationTokenOptionsWrapper(_connection.Options, _cts);

            return new Sql4CdsDataReader(this, options, behavior);
        }
    }
}
