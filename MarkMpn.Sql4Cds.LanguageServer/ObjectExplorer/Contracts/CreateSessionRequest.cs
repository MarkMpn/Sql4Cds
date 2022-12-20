using MarkMpn.Sql4Cds.LanguageServer.Connection.Contracts;
using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// Establishes an Object Explorer tree session for a specific connection.
    /// This will create a connection to a specific server or database, register
    /// it for use in the 
    /// </summary>
    public class CreateSessionRequest
    {
        public const string MessageName = "objectexplorer/createsession";

        public static readonly LspRequest<ConnectionDetails, CreateSessionResponse> Type = new LspRequest<ConnectionDetails, CreateSessionResponse>(MessageName);
    }
}
