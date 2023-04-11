using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ConfirmationResponseParams
    {
        public string OwnerUri { get; set; }

        public bool Result { get; set; }
    }

    public class ConfirmationResponse
    {
        public const string MessageName = "sql4cds/confirm";
        public static readonly LspNotification<ConfirmationResponseParams> Type = new LspNotification<ConfirmationResponseParams>(MessageName);
    }
}
