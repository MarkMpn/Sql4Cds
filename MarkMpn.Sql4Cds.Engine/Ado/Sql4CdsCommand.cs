using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using MarkMpn.Sql4Cds.Engine.Visitors;
using Microsoft.SqlServer.TransactSql.ScriptDom;
#if NETCOREAPP
using Microsoft.PowerPlatform.Dataverse.Client;
#else
using Microsoft.Xrm.Tooling.Connector;
#endif

namespace MarkMpn.Sql4Cds.Engine
{
    /// <summary>
    /// Executes SQL commands against a <see cref="Sql4CdsConnection"/>
    /// </summary>
    public class Sql4CdsCommand : DbCommand
    {
        private Sql4CdsConnection _connection;
        private ExecutionPlanBuilder _planBuilder;
        private string _commandText;
        private bool _useTDSEndpointDirectly;
        private IRootExecutionPlanNode[] _plan;
        private CancellationTokenSource _cts;

        /// <summary>
        /// Creates a new <see cref="Sql4CdsCommand"/>
        /// </summary>
        /// <param name="connection">The <see cref="Sql4CdsConnection"/> to create the command for</param>
        public Sql4CdsCommand(Sql4CdsConnection connection) : this(connection, string.Empty)
        {
        }

        /// <summary>
        /// Creates a new <see cref="Sql4CdsCommand"/>
        /// </summary>
        /// <param name="connection">The <see cref="Sql4CdsConnection"/> to create the command for</param>
        /// <param name="commandText">The SQL command to initialize the new <see cref="Sql4CdsCommand"/> with</param>
        public Sql4CdsCommand(Sql4CdsConnection connection, string commandText)
        {
            _connection = connection;
            CommandText = commandText;
            CommandTimeout = 30;
            DbParameterCollection = new Sql4CdsParameterCollection();

            _planBuilder = new ExecutionPlanBuilder(_connection.DataSources.Values, _connection.Options);
        }

        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                _commandText = value;
                _plan = null;
                _useTDSEndpointDirectly = false;
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

        /// <summary>
        /// Occurs when a statement within a SQL command has finished executing
        /// </summary>
        public event EventHandler<StatementCompletedEventArgs> StatementCompleted;

        internal void OnStatementCompleted(IRootExecutionPlanNode node, int recordsAffected)
        {
            var handler = StatementCompleted;

            if (handler != null)
                handler(this, new StatementCompletedEventArgs(node, recordsAffected));
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
                _planBuilder = new ExecutionPlanBuilder(_connection.DataSources.Values, _connection.Options);
                _plan = null;
                _useTDSEndpointDirectly = false;
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
            if (_useTDSEndpointDirectly || _plan != null)
                return;

            GeneratePlan(true);
        }

        /// <summary>
        /// Prepares the command for execution
        /// </summary>
        /// <param name="useTDSEndpointDirectly">Indicates if the command should be executed directly against the TDS endpoint</param>
        public void Prepare(out bool useTDSEndpointDirectly)
        {
            Prepare();
            useTDSEndpointDirectly = _useTDSEndpointDirectly;
        }

        /// <summary>
        /// Creates the execution plan for the current command
        /// </summary>
        /// <param name="compileForExecution">Indicates if the plan should be generated ready for execution or for display only</param>
        /// <returns>The root nodes of the plan</returns>
        public IRootExecutionPlanNode[] GeneratePlan(bool compileForExecution)
        {
            _planBuilder.CompileConditions = compileForExecution;
            var plan = _planBuilder.Build(CommandText, ((Sql4CdsParameterCollection)Parameters).GetParameterTypes(), out _useTDSEndpointDirectly);

            if (compileForExecution)
                _plan = plan;

            return plan;
        }

        protected override DbParameter CreateDbParameter()
        {
            return CreateParameter();
        }

        public new Sql4CdsParameter CreateParameter()
        {
            return new Sql4CdsParameter();
        }

        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior)
        {
            Prepare();

            if (_useTDSEndpointDirectly)
            {
#if NETCOREAPP
                var svc = (ServiceClient)_connection.DataSources[_connection.Database].Connection;
                var con = new SqlConnection("server=" + svc.ConnectedOrgUriActual.Host);
#else
                var svc = (CrmServiceClient)_connection.DataSources[_connection.Database].Connection;
                var con = new SqlConnection("server=" + svc.CrmConnectOrgUriActual.Host);
#endif
                con.AccessToken = svc.CurrentAccessToken;
                con.Open();

                var cmd = con.CreateCommand();
                cmd.CommandTimeout = (int)TimeSpan.FromMinutes(2).TotalSeconds;
                cmd.CommandText = CommandText;

                if (Parameters.Count > 0)
                {
                    var dom = new TSql150Parser(_connection.Options.QuotedIdentifiers);
                    var fragment = dom.Parse(new StringReader(CommandText), out _);
                    var variables = new VariableCollectingVisitor();
                    fragment.Accept(variables);
                    var requiredParameters = new HashSet<string>(variables.Variables.Select(v => v.Name), StringComparer.OrdinalIgnoreCase);

                    foreach (Sql4CdsParameter sql4cdsParam in Parameters)
                    {
                        if (!requiredParameters.Contains(sql4cdsParam.ParameterName))
                            continue;

                        var param = cmd.CreateParameter();
                        param.ParameterName = sql4cdsParam.ParameterName;

                        if (sql4cdsParam.Value is SqlEntityReference er)
                            param.Value = (SqlGuid)er;
                        else
                            param.Value = sql4cdsParam.Value;

                        cmd.Parameters.Add(param);
                    }
                }

                return new SqlDataReaderWrapper(_connection, this, con, cmd, _connection.Database);
            }

            _cts = CommandTimeout == 0 ? new CancellationTokenSource() : new CancellationTokenSource(TimeSpan.FromSeconds(CommandTimeout));
            
            return new Sql4CdsDataReader(this, _plan, _connection.Options, behavior, _cts.Token);
        }
    }
}
