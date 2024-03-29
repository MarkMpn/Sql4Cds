﻿using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.LanguageServer.Capabilities;
using MarkMpn.Sql4Cds.LanguageServer.Connection;
using MarkMpn.Sql4Cds.LanguageServer.Workspace;
using Microsoft.Extensions.DependencyInjection;
using Newtonsoft.Json.Serialization;
using StreamJsonRpc;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (Array.IndexOf(args, "--enable-remote-debugging-wait") != -1)
                System.Diagnostics.Debugger.Launch();

            var logDirArg = "--log-dir=";
            var logDir = args.Single(a => a.StartsWith(logDirArg)).Substring(logDirArg.Length);

            var formatter = new JsonMessageFormatter();
            formatter.JsonSerializer.ContractResolver = new DefaultContractResolver
            {
                NamingStrategy = new CamelCaseNamingStrategy
                {
                    OverrideSpecifiedNames = false
                }
            };
            var messageHandler = new HeaderDelimitedMessageHandler(Console.OpenStandardOutput(), Console.OpenStandardInput(), formatter);
            var rpc = new JsonRpc(messageHandler);
            
            var serviceCollection = new ServiceCollection();
            serviceCollection.AddSingleton(rpc);
            serviceCollection.AddSingleton<ConnectionManager>();
            serviceCollection.AddSingleton<TextDocumentManager>();
            serviceCollection.AddSingleton<VersionChecker>();
            serviceCollection.AddSingleton(new PersistentMetadataCache(Path.Join(logDir, "Metadata"), rpc));

            var handlerTypes = Assembly
                .GetExecutingAssembly()
                .GetTypes()
                .Where(t => typeof(IJsonRpcMethodHandler).IsAssignableFrom(t) && t.IsClass && !t.IsAbstract)
                .ToList();

            foreach (var handlerType in handlerTypes)
                serviceCollection.AddSingleton(handlerType);

            var services = serviceCollection.BuildServiceProvider();

            foreach (var handlerType in handlerTypes)
            {
                var handler = (IJsonRpcMethodHandler)services.GetService(handlerType);
                handler.Initialize(rpc);
            }

            rpc.StartListening();
            await rpc.Completion;
        }
    }
}
