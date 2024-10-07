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
        private bool _cancelledManually;
        private string _lastDatabase;

        static Sql4CdsCommand()
        {
            // Ensure the FetchXmlScan class is loaded - avoids multithreading issues when using the custom debug visualizer
            // on the command object.
            new FetchXmlScan();
        }

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

            _planBuilder = new ExecutionPlanBuilder(_connection.Session, _connection.Options);
            _planBuilder.Log = msg => _connection.OnInfoMessage(null, msg);
        }

        /// <summary>
        /// Returns the details of the execution plan of the current command
        /// </summary>
        /// <remarks>
        /// This property returns a value based on the state of the command when it was executed or
        /// when <see cref="Prepare"/> was called.
        /// </remarks>
        public IRootExecutionPlanNode[] Plan { get; internal set; }

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

        internal void OnStatementCompleted(IRootExecutionPlanNode node, int recordsAffected, string message)
        {
            _connection.TelemetryClient.TrackEvent("Execute", new Dictionary<string, string> { ["QueryType"] = node.GetType().Name, ["Source"] = _connection.ApplicationName });

            var handler = StatementCompleted;

            if (handler != null)
                handler(this, new StatementCompletedEventArgs(node, recordsAffected, message));
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
                _planBuilder = new ExecutionPlanBuilder(_connection.Session, _connection.Options);
                _planBuilder.Log = msg => _connection.OnInfoMessage(null, msg);
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
                    throw new Sql4CdsException(Sql4CdsError.NotSupported(null, "BEGIN TRAN"));
            }
        }

        internal bool CancelledManually => _cancelledManually;

        public override void Cancel()
        {
            _cancelledManually = true;
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
            if (_lastDatabase == _connection.Database && (UseTDSEndpointDirectly || Plan != null))
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
                _lastDatabase = _connection.Database;

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
                var exProps = new Dictionary<string, string> { ["Sql"] = CommandText, ["Source"] = _connection.ApplicationName };

                if (ex is ISql4CdsErrorException sqlEx && sqlEx.Errors.Count > 0)
                    exProps["ErrorNumber"] = sqlEx.Errors[0].Number.ToString();

                _connection.TelemetryClient.TrackException(ex, exProps);

                if (ex is Sql4CdsException)
                    throw;

                throw new Sql4CdsException(ex.Message, ex);
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
                _cancelledManually = false;
                _cts = CommandTimeout == 0 ? new CancellationTokenSource() : new CancellationTokenSource(TimeSpan.FromSeconds(CommandTimeout));

                if (UseTDSEndpointDirectly)
                {
#if NETCOREAPP
                    var svc = (ServiceClient)_connection.Session.DataSources[_connection.Database].Connection;
                    var con = new SqlConnection("server=" + svc.ConnectedOrgUriActual.Host);
#else
                    var svc = (CrmServiceClient)_connection.Session.DataSources[_connection.Database].Connection;
                    var con = new SqlConnection("server=" + svc.CrmConnectOrgUriActual.Host);
#endif
                    con.AccessToken = svc.CurrentAccessToken;
                    con.Open();

                    var cmd = con.CreateCommand();
                    cmd.CommandTimeout = (int)TimeSpan.FromMinutes(2).TotalSeconds;
                    cmd.CommandText = SqlNode.ApplyCommandBehavior(CommandText, behavior, new NodeExecutionContext(null, _connection.Options, null, null, null));
                    var node = new SqlNode { Sql = cmd.CommandText, DataSource = _connection.Database };
                    cmd.StatementCompleted += (_, e) =>
                    {
                        _connection.Session.GlobalVariableValues["@@ROWCOUNT"] = (SqlInt32)e.RecordCount;
                        OnStatementCompleted(node, e.RecordCount, $"({e.RecordCount} {(e.RecordCount == 1 ? "row" : "rows")} affected)");
                    };

                    if (Parameters.Count > 0)
                    {
                        var dom = new TSql160Parser(_connection.Options.QuotedIdentifiers);
                        var fragment = dom.Parse(new StringReader(CommandText), out _);
                        var variables = new VariableCollectingVisitor();
                        fragment.Accept(variables);
                        var requiredParameters = new HashSet<string>(variables.Variables.Select(v => v.Name), StringComparer.OrdinalIgnoreCase);

                        foreach (Sql4CdsParameter sql4cdsParam in Parameters)
                        {
                            if (!requiredParameters.Contains(sql4cdsParam.FullParameterName))
                                continue;

                            var param = cmd.CreateParameter();
                            param.ParameterName = sql4cdsParam.FullParameterName;

                            if (sql4cdsParam.Value is SqlEntityReference er)
                                param.Value = (SqlGuid)er;
                            else
                                param.Value = sql4cdsParam.Value;

                            cmd.Parameters.Add(param);
                        }
                    }

                    return new SqlDataReaderWrapper(con, cmd, behavior, node, _cts.Token);
                }

                var options = new CancellationTokenOptionsWrapper(_connection.Options, _cts);

                var reader = new Sql4CdsDataReader(this, options, behavior);

                if (CommandType == CommandType.StoredProcedure)
                {
                    // Capture the values of output parameters
                    foreach (var param in Parameters.Cast<Sql4CdsParameter>().Where(p => p.Direction == ParameterDirection.Output))
                        param.SetOutputValue((INullable)reader.ParameterValues[param.FullParameterName]);
                }

                return reader;
            }
            catch (Exception ex)
            {
                var exProps = new Dictionary<string, string> { ["Sql"] = CommandText, ["Source"] = _connection.ApplicationName };

                if (ex is ISql4CdsErrorException sqlEx && sqlEx.Errors.Count > 0)
                    exProps["ErrorNumber"] = sqlEx.Errors[0].Number.ToString();

                _connection.TelemetryClient.TrackException(ex, exProps);
                throw;
            }
        }
    }
}
