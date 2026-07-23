import { dotnet } from './_framework/dotnet.js';

const { getAssemblyExports, getConfig } = await dotnet
  .withDiagnosticTracing(false)
  .create();

const config = getConfig();
const assemblyExports = await getAssemblyExports(config.mainAssemblyName);
const sql4cds = assemblyExports.MarkMpn.Sql4Cds.Engine.Wasm.Sql4CdsExports;

const script = `
CREATE TABLE #Numbers (Id INT, Name NVARCHAR(100));
INSERT INTO #Numbers (Id, Name) VALUES (2, 'World'), (1, 'Hello');
SELECT Id, Name, ROW_NUMBER() OVER (ORDER BY Id) AS RowNum FROM #Numbers;
`;

sql4cds.Reset();

const plan = JSON.parse(sql4cds.Explain(script));
const result = JSON.parse(sql4cds.Execute(script));

document.querySelector('#plan').textContent = JSON.stringify(plan, null, 2);
document.querySelector('#result').textContent = JSON.stringify(result, null, 2);
