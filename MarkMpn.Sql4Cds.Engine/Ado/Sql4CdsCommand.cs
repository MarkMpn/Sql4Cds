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
    public class Sql4CdsCommand : DbCommand
    {
        private Sql4CdsConnection _connection;
        private ExecutionPlanBuilder _planBuilder;
        private string _commandText;
        private CommandType _commandType;
        private CancellationTokenSource _cts;

        public Sql4CdsCommand(Sql4CdsConnection connection) : this(connection, string.Empty)
        {
        }

        public Sql4CdsCommand(Sql4CdsConnection connection, string commandText)
        {
            _connection = connection;
            CommandText = commandText;
            CommandType = CommandType.Text;
            CommandTimeout = 30;
            DbParameterCollection = new Sql4CdsParameterCollection();

            _planBuilder = new ExecutionPlanBuilder(_connection.DataSources.Values, _connection.Options);
        }

        /// <summary>
        /// Returns the details of the execution plan of the current command
        /// </summary>
        /// <remarks>
        /// This property returns a value based on the state of the command when it was executed or
        /// when <see cref="Prepare"/> was called.
        /// </remarks>
        public IRootExecutionPlanNode[] Plan { get; private set; }

        /// <summary>
        /// Indicates if this command can be executed directly against the TDS Endpoint
        /// </summary>
        /// <remarks>
        /// This property returns a value based on the state of the command when it was executed or
        /// when <see cref="Prepare"/> was called.
        /// </remarks>
        public bool UseTDSEndpointDirectly { get; private set; }

        public override string CommandText
        {
            get { return _commandText; }
            set
            {
                _commandText = value;
                Plan = null;
                UseTDSEndpointDirectly = false;
            }
        }

        public override int CommandTimeout { get; set; }
        
        public override CommandType CommandType
        {
            get { return _commandType; }
            set
            {
                if (value != CommandType.Text && value != CommandType.StoredProcedure)
                    throw new ArgumentOutOfRangeException("Only CommandType.Text and CommandType.StoredProcedure are supported");

                _commandType = value;
            }
        }

        public override bool DesignTimeVisible { get; set; }
        
        public override UpdateRowSource UpdatedRowSource { get; set; }

        public event EventHandler<StatementCompletedEventArgs> StatementCompleted;

        internal void OnStatementCompleted(IRootExecutionPlanNode node, int recordsAffected)
        {
            _connection.TelemetryClient.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = node.GetType().Name, ["Source"] = _connection.ApplicationName });

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
                Plan = null;
                UseTDSEndpointDirectly = false;
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
            if (UseTDSEndpointDirectly || Plan != null)
                return;

            GeneratePlan(true);
        }

        /// <summary>
        /// Creates the execution plan for the current command
        /// </summary>
        /// <param name="compileForExecution">Indicates if the plan should be generated ready for execution or for display only</param>
        /// <returns>The root nodes of the plan</returns>
        public IRootExecutionPlanNode[] GeneratePlan(bool compileForExecution)
        {
            try
            {
                _planBuilder.EstimatedPlanOnly = !compileForExecution;

                var commandText = CommandText;

                if (CommandType == CommandType.StoredProcedure)
                {
                    commandText = $"EXECUTE [{CommandText}]";

                    for (var i = 0; i < Parameters.Count; i++)
                    {
                        if (i > 0)
                            commandText += ", ";

                        var param = Parameters[i];
                        commandText += $" {param.ParameterName} = {param.ParameterName}";

                        if (param.Direction == ParameterDirection.Output)
                            commandText += " OUTPUT";
                    }
                }

                var plan = _planBuilder.Build(commandText, ((Sql4CdsParameterCollection)Parameters).GetParameterTypes(), out var useTDSEndpointDirectly);
                UseTDSEndpointDirectly = useTDSEndpointDirectly;

                if (compileForExecution)
                {
                    Plan = plan;
                }
                else
                {
                    foreach (var query in plan)
                        _connection.TelemetryClient.TrackEvent("Convert", new Dictionary<string, string> { ["QueryType"] = query.GetType().Name, ["Source"] = _connection.ApplicationName });
                }

                return plan;
            }
            catch (Exception ex)
            {
                _connection.TelemetryClient.TrackException(ex, new Dictionary<string, string> { ["Sql"] = CommandText, ["Source"] = _connection.ApplicationName });
                throw;
            }
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

            try
            {
                if (UseTDSEndpointDirectly)
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
                    cmd.CommandText = SqlNode.ApplyCommandBehavior(CommandText, behavior, _connection.Options);
                    cmd.StatementCompleted += (_, e) => _connection.GlobalVariableValues["@@ROWCOUNT"] = (SqlInt32)e.RecordCount;

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
                var options = new CancellationTokenOptionsWrapper(_connection.Options, _cts);

                var reader = new Sql4CdsDataReader(this, options, behavior);

                if (CommandType == CommandType.StoredProcedure)
                {
                    // Capture the values of output parameters
                    foreach (var param in Parameters.Cast<Sql4CdsParameter>().Where(p => p.Direction == ParameterDirection.Output))
                        param.SetOutputValue((INullable)reader.ParameterValues[param.ParameterName]);
                }

                return reader;
            }
            catch (Exception ex)
            {
                _connection.TelemetryClient.TrackException(ex, new Dictionary<string, string> { ["Sql"] = CommandText, ["Source"] = _connection.ApplicationName });
                throw;
            }
        }
    }
}
