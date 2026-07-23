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
3. create a SQL 4 CDS session with either `CreateLocalSession()` or `CreateSession(callbackSetId, dataSourceName)`
4. call `Explain(sessionId, sql)` and `Execute(sessionId, sql)`
5. dispose the session with `DisposeSession(sessionId)`

When using `CreateSession`, first register a callback object in JavaScript under a callback-set id, then pass that id to the .NET export. The callback object should match the `IOrganizationService` surface:

* `execute(requestName, requestType, requestJson) => responseJson`
* `retrieveMultiple(queryType, queryJson) => entityCollectionJson`
* `retrieve(logicalName, id, columnSetJson) => entityJson`
* `create(entityJson) => id`
* `update(entityJson)`
* `delete(logicalName, id)`
* `associate(logicalName, id, relationshipJson, relatedEntitiesJson)`
* `disassociate(logicalName, id, relationshipJson, relatedEntitiesJson)`

The callback payloads are JSON produced from the Dataverse SDK types, so JavaScript can inspect the request/query and forward it to Web API, FetchXML, metadata, or other application-specific handlers.
