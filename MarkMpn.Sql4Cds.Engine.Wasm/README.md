# SQL 4 CDS WebAssembly sample

Publish the sample with:

```bash
dotnet workload restore /home/runner/work/Sql4Cds/Sql4Cds/MarkMpn.Sql4Cds.Engine.Wasm/MarkMpn.Sql4Cds.Engine.Wasm.csproj
dotnet publish /home/runner/work/Sql4Cds/Sql4Cds/MarkMpn.Sql4Cds.Engine.Wasm/MarkMpn.Sql4Cds.Engine.Wasm.csproj -c Release
```

Then serve `/home/runner/work/Sql4Cds/Sql4Cds/MarkMpn.Sql4Cds.Engine.Wasm/bin/Release/net8.0/browser-wasm/AppBundle` with any static web server.

`main.js` shows the minimal JavaScript integration:

1. load `dotnet.js`
2. get the exported .NET assembly functions
3. call `Reset`, `Explain`, and `Execute`
4. parse the returned JSON
