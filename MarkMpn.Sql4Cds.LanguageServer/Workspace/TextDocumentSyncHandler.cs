using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer.Workspace
{
    internal class TextDocumentHandler : IJsonRpcMethodHandler
    {
        private readonly TextDocumentManager _documentManager;

        public TextDocumentHandler(TextDocumentManager documentManager)
        {
            _documentManager = documentManager;
        }

        public void Initialize(JsonRpc lsp)
        {
            lsp.AddHandler(Methods.TextDocumentDidOpen, HandleTextDocumentDidOpen);
            lsp.AddHandler(Methods.TextDocumentDidChange, HandleTextDocumentDidChange);
            lsp.AddHandler(Methods.TextDocumentDidClose, HandleTextDocumentDidClose);
        }

        public TextDocumentSyncKind Change { get; } = TextDocumentSyncKind.Full;

        public void HandleTextDocumentDidOpen(DidOpenTextDocumentParams notification)
        {
            _documentManager.SetContent(notification.TextDocument.Uri.ToString(), notification.TextDocument.Text);
        }

        public void HandleTextDocumentDidChange(DidChangeTextDocumentParams notification)
        {
            _documentManager.SetContent(notification.TextDocument.Uri.ToString(), notification.ContentChanges.Single().Text);
        }

        public void HandleTextDocumentDidClose(DidCloseTextDocumentParams notification)
        {
            _documentManager.SetContent(notification.TextDocument.Uri.ToString(), null);
        }
    }
}
