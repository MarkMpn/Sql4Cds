using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.ObjectExplorer.Contracts
{
    /// <summary>
    /// Information returned when a session is disconnected.
    /// Contains success information and a <see cref="SessionId"/>
    /// </summary>
    public class SessionDisconnectedParameters
    {
        /// <summary>
        /// Boolean indicating if the connection was successful
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Unique ID to use when sending any requests for objects in the
        /// tree under the node
        /// </summary>
        public string SessionId { get; set; }

        /// <summary>
        /// Error message returned from the engine for a object explorer session failure reason, if any.
        /// </summary>
        public string ErrorMessage { get; set; }
    }

    /// <summary>
    /// Session disconnected notification
    /// </summary>
    public class SessionDisconnectedNotification
    {
        public const string MessageName = "objectexplorer/sessiondisconnected";

        public static readonly LspNotification<SessionDisconnectedParameters> Type = new LspNotification<SessionDisconnectedParameters>(MessageName);
    }
}
