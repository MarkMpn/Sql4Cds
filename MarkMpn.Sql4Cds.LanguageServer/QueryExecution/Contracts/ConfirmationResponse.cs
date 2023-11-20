using Microsoft.VisualStudio.LanguageServer.Protocol;

namespace MarkMpn.Sql4Cds.LanguageServer.QueryExecution.Contracts
{
    public class ConfirmationResponseParams
    {
        public string OwnerUri { get; set; }

        public ConfirmationResult Result { get; set; }
    }

    public enum ConfirmationResult
    {
        No,
        Yes,
        All
    }

    public class ConfirmationResponse
    {
        public const string MessageName = "sql4cds/confirm";
        public static readonly LspNotification<ConfirmationResponseParams> Type = new LspNotification<ConfirmationResponseParams>(MessageName);
    }
}
