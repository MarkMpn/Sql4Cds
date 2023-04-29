using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ConfirmationParams
    {
        public string OwnerUri { get; set; }

        public string Msg { get; set; }
    }

    public class ConfirmationRequest
    {
        public const string MessageName = "sql4cds/confirmation";
        public static readonly LspNotification<ConfirmationParams> Type = new LspNotification<ConfirmationParams>(MessageName);
    }
}
