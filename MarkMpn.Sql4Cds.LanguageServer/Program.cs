using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OmniSharp.Extensions.LanguageServer.Protocol.Models;
using OmniSharp.Extensions.LanguageServer.Server;

namespace MarkMpn.Sql4Cds.LanguageServer
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (Array.IndexOf(args, "--enable-remote-debugging-wait") != -1)
                System.Diagnostics.Debugger.Launch();

            var server = await OmniSharp.Extensions.LanguageServer.Server.LanguageServer.From(options =>
            {
                options
                    .WithInput(Console.OpenStandardInput())
                    .WithOutput(Console.OpenStandardOutput())
                    .ConfigureLogging(
                        x => x
                            .AddLanguageProtocolLogging()
                            .SetMinimumLevel(LogLevel.Debug)
                    )
                    //.WithConfigurationSection("SQL4CDS")
                    //.WithConfigurationItem(new OmniSharp.Extensions.LanguageServer.Protocol.Models.ConfigurationItem
                    //{
                    //    Section = "SQL4CDS"
                    //})
                    .WithHandler<TextDocumentHandler>()
                    .WithHandler<CapabilitiesHandler>()
                    .WithHandler<ConnectionHandler>()
                    .WithHandler<CreateSessionHandler>()
                    .WithHandler<ExpandHandler>()
                    .WithHandler<ExecuteHandler>()
                    .WithHandler<GetDatabaseInfoHandler>()
                    .WithHandler<ConfigurationHandler>()
                    .WithServices(x =>
                    {
                        x.AddLogging(b => b.SetMinimumLevel(LogLevel.Trace));
                        x.AddSingleton<ConnectionManager>();
                        x.AddSingleton<TextDocumentManager>();
                        //x.AddSingleton(new ConfigurationItem { Section = "SQL4CDS" });
                    });
            });
            await server.WaitForExit;
        }
    }
}
