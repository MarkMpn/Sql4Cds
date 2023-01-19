using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;

namespace MarkMpn.Sql4Cds.LanguageServer.Workspace
{
    class TextDocumentManager
    {
        private ConcurrentDictionary<string, string> _contents;

        public TextDocumentManager()
        {
            _contents = new ConcurrentDictionary<string, string>();
        }

        public void SetContent(string documentUri, string content)
        {
            if (content == null)
                _contents.TryRemove(documentUri, out _);
            else
                _contents[documentUri] = content;
        }

        public string GetContent(string documentUri)
        {
            return _contents[documentUri];
        }
    }
}
