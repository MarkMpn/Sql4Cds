using System;
using System.Collections.Generic;
using System.ComponentModel.Design;
using System.Data.SqlClient;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using EnvDTE;
using EnvDTE80;
using MarkMpn.Sql4Cds.Engine;
using Microsoft.SqlServer.Management.UI.VSIntegration;
using Microsoft.SqlServer.Management.UI.VSIntegration.ObjectExplorer;
using Microsoft.VisualStudio.CommandBars;
using Microsoft.VisualStudio.Shell;
using Microsoft.VisualStudio.Shell.Interop;
using Microsoft.Xrm.Tooling.Connector;
using Task = System.Threading.Tasks.Task;

namespace MarkMpn.Sql4Cds.SSMS
{
    /// <summary>
    /// Command handler
    /// </summary>
    internal sealed class Sql2FetchXmlCommand
    {
        /// <summary>
        /// Command ID.
        /// </summary>
        public const int CommandId = 0x0100;

        /// <summary>
        /// Command menu group (command set GUID).
        /// </summary>
        public static readonly Guid CommandSet = new Guid("fd809e45-c5a9-40cc-9f78-501dd3f71817");

        /// <summary>
        /// VS Package that provides this command, not null.
        /// </summary>
        private readonly AsyncPackage package;

        private readonly DTE2 _dte;
        private readonly IObjectExplorerService _objEx;

        /// <summary>
        /// Initializes a new instance of the <see cref="Sql2FetchXmlCommand"/> class.
        /// Adds our command handlers for menu (commands must exist in the command table file)
        /// </summary>
        /// <param name="package">Owner package, not null.</param>
        /// <param name="commandService">Command service to add command to, not null.</param>
        private Sql2FetchXmlCommand(AsyncPackage package, OleMenuCommandService commandService, DTE2 dte, IObjectExplorerService objEx)
        {
            this.package = package ?? throw new ArgumentNullException(nameof(package));
            commandService = commandService ?? throw new ArgumentNullException(nameof(commandService));

            var menuCommandID = new CommandID(CommandSet, CommandId);
            var menuItem = new OleMenuCommand(this.Execute, menuCommandID);
            menuItem.BeforeQueryStatus += QueryStatus;
            commandService.AddCommand(menuItem);

            _dte = dte;
            _objEx = objEx;
        }

        private void QueryStatus(object sender, EventArgs e)
        {
            var menuItem = (OleMenuCommand)sender;

            if (_dte.ActiveDocument == null)
            {
                menuItem.Enabled = false;
                return;
            }

            var textDoc = (TextDocument)_dte.ActiveDocument.Object("TextDocument");

            if (textDoc == null)
            {
                menuItem.Enabled = false;
                return;
            }

            menuItem.Enabled = true;
        }

        /// <summary>
        /// Gets the instance of the command.
        /// </summary>
        public static Sql2FetchXmlCommand Instance
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the service provider from the owner package.
        /// </summary>
        private Microsoft.VisualStudio.Shell.IAsyncServiceProvider ServiceProvider
        {
            get
            {
                return this.package;
            }
        }

        /// <summary>
        /// Initializes the singleton instance of the command.
        /// </summary>
        /// <param name="package">Owner package, not null.</param>
        public static async Task InitializeAsync(AsyncPackage package, DTE2 dte, IObjectExplorerService objEx)
        {
            // Verify the current thread is the UI thread - the call to AddCommand in Sql2FetchXmlCommand's constructor requires
            // the UI thread.
            ThreadHelper.ThrowIfNotOnUIThread();
            
            OleMenuCommandService commandService = await package.GetServiceAsync((typeof(IMenuCommandService))) as OleMenuCommandService;
            Instance = new Sql2FetchXmlCommand(package, commandService, dte, objEx);
        }

        /// <summary>
        /// This function is the callback used to execute the command when the menu item is clicked.
        /// See the constructor to see how the menu item is associated with this function using
        /// OleMenuCommandService service and MenuCommand class.
        /// </summary>
        /// <param name="sender">Event sender.</param>
        /// <param name="e">Event args.</param>
        private void Execute(object sender, EventArgs e)
        {
            ThreadHelper.ThrowIfNotOnUIThread();

            var textDoc = (TextDocument) _dte.ActiveDocument.Object("TextDocument");

            _objEx.GetSelectedNodes(out var size, out var nodes);
            if (size == 0)
                return;

            var node = nodes[0];

            while (node.UrnPath != "Server/Database" && node.UrnPath != "Server")
                node = node.Parent;

            if (node == null)
                return;

            var sql = textDoc.Selection.Text;

            if (String.IsNullOrEmpty(sql))
            {
                var startPoint = textDoc.StartPoint.CreateEditPoint();
                sql = startPoint.GetText(textDoc.EndPoint);
            }

            var conStr = new SqlConnectionStringBuilder(node.Connection.ConnectionString);
            var server = conStr.DataSource.Split(',')[0];
            var resource = $"https://{server}/";
            var req = WebRequest.CreateHttp(resource);
            req.AllowAutoRedirect = false;
            var resp = req.GetResponse();
            var authority = new UriBuilder(resp.Headers[HttpResponseHeader.Location]);
            authority.Query = "";
            authority.Port = -1;
            var authParams = new AuthParams(conStr.Authentication, server, conStr.InitialCatalog, resource, authority.ToString(), conStr.UserID, "", Guid.Empty);
            AuthOverrideHook.Instance.AddAuth(authParams);
            
            CrmServiceClient.AuthOverrideHook = AuthOverrideHook.Instance;
            var con = new CrmServiceClient(new Uri(resource), true);

            var sql2FetchXml = new Sql2FetchXml(new AttributeMetadataCache(con), false);
            sql2FetchXml.ColumnComparisonAvailable = true;
            sql2FetchXml.TSqlEndpointAvailable = false;

            try
            {
                var queries = sql2FetchXml.Convert(sql);

                foreach (var query in queries.OfType<FetchXmlQuery>())
                {
                    var window = _dte.ItemOperations.NewFile("General\\XML File");
                    var doc = (TextDocument)window.Document.Object("TextDocument");

                    var editPoint = doc.EndPoint.CreateEditPoint();
                    editPoint.Insert("<!-- Created from query:\r\n\r\n");
                    editPoint.Insert(query.Sql);

                    if (query.Extensions.Count > 0)
                    {
                        editPoint.Insert("‼ WARNING ‼\r\n");
                        editPoint.Insert("This query requires additional processing. This FetchXML gives the required data, but needs additional processing to format it in the same way as returned by the TDS Endpoint or SQL 4 CDS.\r\n\r\n");
                        editPoint.Insert("Learn more at https://markcarrington.dev/sql-4-cds/additional-processing/\r\n\r\n");
                    }

                    if (query.DistinctWithoutSort)
                    {
                        editPoint.Insert("‼ WARNING ‼\r\n");
                        editPoint.Insert("This DISTINCT query does not have a sort order applied. Unexpected results may be returned when the results are split over multiple pages. Add a sort order to retrieve the correct results.\r\n");
                        editPoint.Insert("Learn more at https://docs.microsoft.com/powerapps/developer/common-data-service/org-service/paging-behaviors-and-ordering#ordering-with-a-paging-cookie\r\n\r\n");
                    }

                    editPoint.Insert("-->\r\n\r\n");
                    editPoint.Insert(query.FetchXmlString);
                }
            }
            catch (NotSupportedQueryFragmentException ex)
            {
                VsShellUtilities.ShowMessageBox(package, "The query could not be converted to FetchXML: " + ex.Message, "Query Not Supported", OLEMSGICON.OLEMSGICON_CRITICAL, OLEMSGBUTTON.OLEMSGBUTTON_OK, OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
            }
            catch (QueryParseException ex)
            {
                VsShellUtilities.ShowMessageBox(package, "The query could not be parsed: " + ex.Message, "Query Parsing Error", OLEMSGICON.OLEMSGICON_CRITICAL, OLEMSGBUTTON.OLEMSGBUTTON_OK, OLEMSGDEFBUTTON.OLEMSGDEFBUTTON_FIRST);
            }
        }

        class AuthParams : SqlAuthenticationParameters
        {
            public AuthParams(SqlAuthenticationMethod authenticationMethod, string serverName, string databaseName, string resource, string authority, string userId, string password, Guid connectionId) : base(authenticationMethod, serverName, databaseName, resource, authority, userId, password, connectionId)
            {
            }
        }

        class AuthOverrideHook : IOverrideAuthHookWrapper
        {
            private IDictionary<string, AuthParams> _authParams = new Dictionary<string, AuthParams>();

            public static AuthOverrideHook Instance { get; } = new AuthOverrideHook();

            public void AddAuth(AuthParams authParams)
            {
                _authParams[authParams.Resource] = authParams;
            }

            public string GetAuthToken(Uri connectedUri)
            {
                var uri = new Uri(connectedUri, "/");
                var authParams = _authParams[uri.ToString()];
                var authProv = SqlAuthenticationProvider.GetProvider(authParams.AuthenticationMethod);
                var token = Task.Run(() => authProv.AcquireTokenAsync(authParams)).ConfigureAwait(false).GetAwaiter().GetResult();

                return token.AccessToken;
            }
        }
    }
}
