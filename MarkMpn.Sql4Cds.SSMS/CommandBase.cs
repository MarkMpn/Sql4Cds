using System;
using System.Collections.Generic;
#if System_Data_SqlClient
using System.Data.SqlClient;
#else
using Microsoft.Data.SqlClient;
#endif
using System.Net;
using EnvDTE;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.ApplicationInsights;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.VisualStudio.Shell;
using Microsoft.Xrm.Tooling.Connector;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;

namespace MarkMpn.Sql4Cds.SSMS
{
    internal abstract class CommandBase
    {
        private static readonly IDictionary<string, CrmServiceClient> _clientCache = new Dictionary<string,CrmServiceClient>();
        private static readonly IDictionary<string, AttributeMetadataCache> _metadataCache = new Dictionary<string, AttributeMetadataCache>();
        private static readonly IDictionary<string, ITableSizeCache> _tableSizeCache = new Dictionary<string, ITableSizeCache>();
        private static readonly IDictionary<string, IMessageCache> _messageCache = new Dictionary<string, IMessageCache>();

        protected static readonly TelemetryClient _ai;

        static CommandBase()
        {
            _ai = new TelemetryClient(new Microsoft.ApplicationInsights.Extensibility.TelemetryConfiguration("79761278-a908-4575-afbf-2f4d82560da6"));
        }

        protected CommandBase(Sql4CdsPackage package, DTE2 dte)
        {
            Package = package;
            Dte = dte;
        }

        /// <summary>
        /// The package that this command was loaded from
        /// </summary>
        protected Sql4CdsPackage Package { get; }

        /// <summary>
        /// Returns the currently active text document
        /// </summary>
        protected TextDocument ActiveDocument
        {
            get
            {
                ThreadHelper.ThrowIfNotOnUIThread();
                return (TextDocument)Dte.ActiveDocument?.Object("TextDocument");
            }
        }

        protected DTE2 Dte { get; }

        /// <summary>
        /// Gets the query text that would be executed based on the current selection
        /// </summary>
        /// <returns></returns>
        protected string GetQuery()
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var scriptFactory = new ScriptFactoryWrapper(ServiceCache.ScriptFactory);
            var sqlScriptEditorControl = scriptFactory.GetCurrentlyActiveFrameDocView(ServiceCache.VSMonitorSelection, false, out _);
            var textSpan = sqlScriptEditorControl.GetSelectedTextSpan();

            return textSpan.Text;
        }

        /// <summary>
        /// Gets the details of the currently active connection
        /// </summary>
        /// <returns></returns>
        protected SqlConnectionStringBuilder GetConnectionInfo(bool log)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var scriptFactory = new ScriptFactoryWrapper(ServiceCache.ScriptFactory);
            var sqlScriptEditorControl = scriptFactory.GetCurrentlyActiveFrameDocView(ServiceCache.VSMonitorSelection, false, out _);

            if (sqlScriptEditorControl?.ConnectionString == null)
            {
                if (log)
                    VsShellUtilities.LogMessage("SQL 4 CDS", "No currently active window or connection", Microsoft.VisualStudio.Shell.Interop.__ACTIVITYLOG_ENTRYTYPE.ALE_INFORMATION);

                return null;
            }
            
            return new SqlConnectionStringBuilder(sqlScriptEditorControl.ConnectionString);
        }

        /// <summary>
        /// Checks if the current connection is to a Dataverse instance
        /// </summary>
        /// <returns></returns>
        protected bool IsDataverse()
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var conStr = GetConnectionInfo(false);

            return IsDataverse(conStr);
        }

        /// <summary>
        /// Checks if the given connection is to a Dataverse instance
        /// </summary>
        /// <param name="conStr"></param>
        /// <returns></returns>
        protected bool IsDataverse(SqlConnectionStringBuilder conStr)
        {
            if (conStr == null)
                return false;

            var serverParts = conStr.DataSource.Split(',');

            if (serverParts.Length > 2)
                return false;

            if (!serverParts[0].EndsWith(".dynamics.com"))
                return false;

            if (serverParts.Length > 1 && serverParts[1] != "5558")
                return false;

            return true;
        }

        /// <summary>
        /// Gets the Dataverse connection details for the active query
        /// </summary>
        /// <returns>The <see cref="DataSource"/> instance representing this connection</returns>
        protected DataSource GetDataSource()
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var conStr = GetConnectionInfo(true);
            var name = conStr.DataSource.Split('.')[0];
            var con = ConnectCDS(conStr);
            var metadata = GetMetadataCache(conStr);
            var tableSizeCache = GetTableSizeCache(conStr, metadata);
            var messageCache = GetMessageCache(conStr, metadata);

            return new DataSource
            {
                Connection = con,
                Metadata = metadata,
                Name = name,
                TableSizeCache = tableSizeCache,
                MessageCache = messageCache
            };
        }

        /// <summary>
        /// Connects to the Dataverse API for the active query
        /// </summary>
        /// <returns></returns>
        protected CrmServiceClient ConnectCDS()
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            // Get the server name based on the current SQL connection
            var conStr = GetConnectionInfo(true);
            return ConnectCDS(conStr);
        }

        /// <summary>
        /// Connects to the Dataverse API for the given SQL connection
        /// </summary>
        /// <returns></returns>
        protected CrmServiceClient ConnectCDS(SqlConnectionStringBuilder conStr)
        {
            if (conStr == null)
                return null;

            var server = conStr.DataSource.Split(',')[0];

            if (_clientCache.TryGetValue(server, out var con))
                return con.Clone();

            var resource = $"https://{server}/";
            var req = WebRequest.CreateHttp(resource);
            req.AllowAutoRedirect = false;
            var resp = req.GetResponse();
            var authority = new UriBuilder(resp.Headers[HttpResponseHeader.Location]);
            authority.Query = "";
            authority.Port = -1;

#if System_Data_SqlClient
            var authParams = new AuthParams(conStr.Authentication, server, conStr.InitialCatalog, resource, authority.ToString(), conStr.UserID, "", Guid.Empty);
#else
            var authParams = new AuthParams(conStr.Authentication, server, conStr.InitialCatalog, resource, authority.ToString(), conStr.UserID, "", Guid.Empty, conStr.ConnectTimeout);
#endif

            AuthOverrideHook.Instance.AddAuth(authParams);

            con = new CrmServiceClient(new Uri(resource), true);
            _clientCache[server] = con;

            return con.Clone();
        }

        /// <summary>
        /// Gets metadata details for this connection
        /// </summary>
        /// <returns></returns>
        protected AttributeMetadataCache GetMetadataCache()
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            // Get the server name based on the current SQL connection
            var conStr = GetConnectionInfo(true);
            return GetMetadataCache(conStr);
        }

        /// <summary>
        /// Gets metadata details for the given connection
        /// </summary>
        /// <returns></returns>
        protected AttributeMetadataCache GetMetadataCache(SqlConnectionStringBuilder conStr)
        {
            if (conStr == null)
                return null;

            var server = conStr.DataSource.Split(',')[0];

            if (_metadataCache.TryGetValue(server, out var metadata))
                return metadata;

            metadata = new AttributeMetadataCache(ConnectCDS(conStr));
            _metadataCache[server] = metadata;

            return metadata;
        }

        private ITableSizeCache GetTableSizeCache(SqlConnectionStringBuilder conStr, IAttributeMetadataCache metadata)
        {
            if (conStr == null)
                return null;

            var server = conStr.DataSource.Split(',')[0];

            if (_tableSizeCache.TryGetValue(server, out var tableSizeCache))
                return tableSizeCache;

            tableSizeCache = new TableSizeCache(ConnectCDS(conStr), metadata);
            _tableSizeCache[server] = tableSizeCache;

            return tableSizeCache;
        }

        private IMessageCache GetMessageCache(SqlConnectionStringBuilder conStr, IAttributeMetadataCache metadata)
        {
            if (conStr == null)
                return null;

            var server = conStr.DataSource.Split(',')[0];

            if (_messageCache.TryGetValue(server, out var messageCache))
                return messageCache;

            messageCache = new MessageCache(ConnectCDS(conStr), metadata);
            _messageCache[server] = messageCache;

            return messageCache;
        }
    }
}
