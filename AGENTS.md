# Structure

The project is primarily composed of:
- a core engine `MarkMpn.Sql4Cds.Engine` which handles executing a query
- `MarkMpn.Sql4Cds.XTB` which exposes the engine in an XrmToolBox tool (wrapped by `MarkMpn.Sql4Cds`)
- `MarkMpn.Sql4Cds.LanguageServer` which exposes the engine to LSP clients
- `MarkMpn.Sql4Cds.PPTB` which provides a Power Platform ToolBox client that consumes the language server
- `VSCodeExtension` which provides a VS Code extension that consumes the language server
- `MarkMpn.Sql4Cds.SSMS` which provides core classes for interop with the Microsoft SSMS client
- `MarkMpn.Sql4Cds.SSMS.{version}` provides integrations with a specific version of the SSMS client

## Goals

The query execution should follow the rules of T-SQL, translating the query to native Dataverse requests wherever possible.
Where direct translation is not possible, the query should be executed in memory to preserve the T-SQL semantics.

Performance is paramount. Retrieving large amounts of data, or small amounts across large numbers of requests, is much less
efficient than pushing down the query details to the Dataverse requests to be executed by the server.

UI consistency with standard Microsoft tooling such as SSMS is also important. Some variation to fit the host environments
such as XrmToolBox, VS Code or Power Platform ToolBox is inevitable, but consistency should be prioritised.

As many Dataverse and T-SQL features as possible should be available. Where a T-SQL feature is not implemented, an explicit
error should be generated so the user is not misled into thinking their query will have the effect expected by using the
unsupported option.

## Non-Goals

Custom extensions to the T-SQL language in order to support Dataverse features with no T-SQL analogue are not expected.
Any deviations from the documented T-SQL language must still conform to the same grammar, such as introducing custom
string query hints or data types, but these should be kept to a minimum.