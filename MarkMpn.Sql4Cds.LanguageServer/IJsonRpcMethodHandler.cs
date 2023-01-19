using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    internal interface IJsonRpcMethodHandler
    {
        void Initialize(JsonRpc lsp);
    }
}
