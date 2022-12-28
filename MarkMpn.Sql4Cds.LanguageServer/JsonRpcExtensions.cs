using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualStudio.LanguageServer.Protocol;
using Newtonsoft.Json.Linq;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    static class JsonRpcExtensions
    {
        public static void AddHandler<TIn,TOut>(this JsonRpc lsp, LspRequest<TIn,TOut> request, Func<TIn,TOut> handler)
        {
            lsp.AddLocalRpcMethod(request.Name, (JToken token) => handler(token.ToObject<TIn>()));
        }

        public static void AddHandler<TIn>(this JsonRpc lsp, LspNotification<TIn> notification, Action<TIn> handler)
        {
            lsp.AddLocalRpcMethod(notification.Name, (JToken token) => handler(token.ToObject<TIn>()));
        }

        public static Task NotifyAsync<TIn>(this JsonRpc lsp, LspNotification<TIn> notification, TIn param)
        {
            return lsp.NotifyAsync(notification.Name, param);
        }
    }
}
