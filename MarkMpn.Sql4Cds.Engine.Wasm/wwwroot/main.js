import { dotnet } from './_framework/dotnet.js';

globalThis.sql4cdsCallbackSets ??= {};

globalThis.sql4cdsInvokeCallback = (callbackSetId, callbackName, arg1, arg2, arg3, arg4) => {
  const callbacks = globalThis.sql4cdsCallbackSets?.[callbackSetId];
  const callback = callbacks?.[callbackName];

  if (typeof callback !== 'function') {
    throw new Error(`No JavaScript callback named '${callbackName}' was supplied.`);
  }

  const args = [arg1, arg2, arg3, arg4].filter(arg => arg !== null && arg !== undefined);
  const result = callback.apply(callbacks, args);

  return result ?? '';
};

const { getAssemblyExports, getConfig } = await dotnet
  .withDiagnosticTracing(false)
  .create();

const config = getConfig();
const assemblyExports = await getAssemblyExports(config.mainAssemblyName);
const sql4cds = assemblyExports.MarkMpn.Sql4Cds.Engine.Wasm.Sql4CdsExports;

const callbacks = {
  execute(requestName, requestType, requestJson) {
    console.log('execute callback', requestName, requestType, requestJson);
    throw new Error(`No JavaScript implementation registered for ${requestName}`);
  },
  retrieveMultiple(queryType, queryJson) {
    console.log('retrieveMultiple callback', queryType, queryJson);
    throw new Error(`No JavaScript implementation registered for ${queryType}`);
  },
  retrieve(logicalName, id, columnSetJson) {
    console.log('retrieve callback', logicalName, id, columnSetJson);
    throw new Error(`No JavaScript implementation registered for retrieve(${logicalName})`);
  },
  create(entityJson) {
    console.log('create callback', entityJson);
    throw new Error('No JavaScript implementation registered for create');
  },
  update(entityJson) {
    console.log('update callback', entityJson);
    throw new Error('No JavaScript implementation registered for update');
  },
  delete(logicalName, id) {
    console.log('delete callback', logicalName, id);
    throw new Error('No JavaScript implementation registered for delete');
  },
  associate(logicalName, id, relationshipJson, relatedEntitiesJson) {
    console.log('associate callback', logicalName, id, relationshipJson, relatedEntitiesJson);
    throw new Error('No JavaScript implementation registered for associate');
  },
  disassociate(logicalName, id, relationshipJson, relatedEntitiesJson) {
    console.log('disassociate callback', logicalName, id, relationshipJson, relatedEntitiesJson);
    throw new Error('No JavaScript implementation registered for disassociate');
  }
};

const callbackSetId = 'sample';
globalThis.sql4cdsCallbackSets[callbackSetId] = callbacks;

const script = `
CREATE TABLE #Numbers (Id INT, Name NVARCHAR(100));
INSERT INTO #Numbers (Id, Name) VALUES (2, 'World'), (1, 'Hello');
SELECT Id, Name, ROW_NUMBER() OVER (ORDER BY Id) AS RowNum FROM #Numbers;
`;

const sessionId = sql4cds.CreateSession(callbackSetId, 'sample');

const plan = JSON.parse(sql4cds.Explain(sessionId, script));
const result = JSON.parse(sql4cds.Execute(sessionId, script));
sql4cds.DisposeSession(sessionId);

document.querySelector('#plan').textContent = JSON.stringify(plan, null, 2);
document.querySelector('#result').textContent = JSON.stringify(result, null, 2);
