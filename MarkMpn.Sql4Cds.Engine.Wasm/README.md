# SQL 4 CDS WebAssembly sample

Publish the sample with:

```bash
dotnet workload restore MarkMpn.Sql4Cds.Engine.Wasm/MarkMpn.Sql4Cds.Engine.Wasm.csproj
dotnet publish MarkMpn.Sql4Cds.Engine.Wasm/MarkMpn.Sql4Cds.Engine.Wasm.csproj -c Release
```

Then serve `MarkMpn.Sql4Cds.Engine.Wasm/bin/Release/net8.0/wwwroot` with any static web server.

`wwwroot/main.js` shows the minimal JavaScript integration:

1. load `dotnet.js`
2. get the exported .NET assembly functions
3. call `Reset`, `Explain`, and `Execute`
4. parse the returned JSON
