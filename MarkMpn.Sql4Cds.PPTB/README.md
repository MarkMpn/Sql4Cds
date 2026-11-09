# SQL 4 CDS for Power Platform ToolBox

Query and modify Dataverse data using standard SQL syntax inside [Power Platform ToolBox](https://www.powerplatformtoolbox.com/) (PPTB).

All standard table data is available to query using `SELECT` statements. Data can also be modified using `INSERT`, `UPDATE`
and `DELETE` statements. Most Dataverse messages can also be invoked using either stored procedure or table-valued function
syntax.

## Examples

```sql
-- Simple SELECT statement
SELECT TOP 10
       name
FROM   account
WHERE  ownerid = CURRENT_USER

-- Add a record using INSERT
INSERT INTO account (name)
VALUES ('Hello from PPTB!')

-- UPDATE existing records
UPDATE account
SET    name = 'PPTB'
WHERE  name = 'XrmToolBox'

-- DELETE unwanted records
DELETE account
WHERE  name = 'MSCRM'

-- Compare records across instances
SELECT uat_users.fullname, prod_users.fullname
FROM   uat.dbo.systemuser AS uat_users
       FULL OUTER JOIN
       prod.dbo.systemuser AS prod_users
       ON uat_users.domainname = prod_users.domainname
WHERE  uat_users.fullname IS DISTINCT FROM prod_users.fullname

-- Copy missing records across instances
INSERT INTO prod.dbo.accounts (name)
SELECT name
FROM   uat.dbo.accounts uat_accounts
       LEFT OUTER JOIN
       prod.dbo.accounts prod_accounts
       ON uat_accounts.name = prod_accounts.name
WHERE  prod_accounts.accountid IS NULL

-- Query metadata
SELECT logicalname, displayname
FROM   metadata.entity

-- Execute messages
EXEC WhoAmI

DECLARE @accountid EntityReference
SELECT TOP 1 @accountid = accountid FROM account
SELECT * FROM RetrieveRecordChangeHistory(@accountid)
```

## Features

A wide variety of SQL Server compatible T-SQL syntax is supported, see the [SQL 4 CDS wiki](https://github.com/MarkMpn/Sql4Cds/wiki) for details.

### Query Editing

A variety of tools are available to help you write queries quickly and accurately.

As you type a query, autocomplete dropdowns may appear to show possible table names, column names and other snippets. Press `Ctrl+Space` to open it at any point.

Hover the mouse over a table, column, function or stored procedure name to view a tooltip with more details.

Queries can be saved and opened to allow you to reuse them. By default, SQL 4 CDS also remembers your session - any open query tabs, including unsaved changes and the
connection each tab was using, are reopened the next time the tool starts. Turn off the "Remember session" option in the settings if you'd prefer not to persist your
queries. With that option off, if you close SQL 4 CDS while you have unsaved changes you'll be prompted to cancel the closure to give you chance to save them, though you
can always click "Ignore & Close" to close the tool anyway without saving. The separate "Remember session connections" option controls whether each reopened tab
reconnects to the connection it was using in the previous session.

To help keep queries consistent, use the "Format" toolbar button. Your query will be adjusted with standard line breaks, spacing and other rules enforced, but the structure
of the query itself will remain unchanged.

### Receiving FetchXML from other tools

Other PPTB tools can send a FetchXML query to SQL 4 CDS. It is converted using the same engine converter as the XrmToolBox version and opened in a new editable SQL tab connected to the caller's inherited primary connection. Existing tabs are preserved, and the query is not executed automatically.

```typescript
await window.toolboxAPI.invocation.launchTool(
    "@markmpn/pptb-sql4cds",
    { fetchXml: '<fetch><entity name="account"><attribute name="name" /></entity></fetch>' },
    { noReturn: true },
);
```

Tools can also discover SQL 4 CDS through the `fetchxml` capability. Invalid FetchXML or conversion failures are shown as notifications.

### MCP / AI assistant integration

SQL 4 CDS is discoverable and invokable through PPTB's MCP server. Runs are **windowed**, not headless, so you can review queries and approve data modifications. Enable and connect to the MCP server in PPTB to use it from an assistant.

Inputs:

| Input | Description |
|--|--|
| `sql` | SQL to open or execute. Provide either `sql` or `fetchXml`, not both. |
| `fetchXml` | FetchXML to convert to SQL before opening or executing it. |
| `execute` | Defaults to `false`. Set to `true` to request execution after user approval. |
| `maxResponseBytes` | UTF-8 JSON response budget, default 65,536 bytes; accepts 1,024 to 1,048,576 bytes. Adjust to the calling assistant's available context. |

The inherited primary connection is used. Every execution first requires approval of the SQL, including stored procedure calls. During execution, data modification prompts are required even if your usual warning thresholds would suppress them. The existing "Yes to all" option applies only to that execution. DELETE/UPDATE safety settings still apply.

Both one-way and two-way invocation modes are supported. One-way calls leave the query/results visible. Two-way calls return converted `sql` or execution `resultSets` and `messages`, with a `status` of `success`, `error`, or `cancelled`. PPTB closes the invoked window when results are returned. Existing tabs are saved according to your session settings.

Execution responses contain column metadata and rows of displayed string values (`null` for SQL NULL). Each result set includes `rowCount`, `returnedRowCount`, and `truncated`. The number of returned rows adapts to their actual serialized size, including column count, UTF-8 characters, JSON escaping, metadata, and messages. A top-level `truncated` flag also reports omitted messages. Individual cell values are not silently shortened. Query execution is not limited by this response budget; full results remain in the tool until it closes. If metadata alone cannot fit, an explicit error is returned.

For example, an assistant can invoke the tool with:

```json
{
    "sql": "SELECT TOP 100 name, accountid FROM account",
    "execute": true,
    "maxResponseBytes": 65536
}
```

The declared two-way timeout is five minutes; callers can request a longer timeout when user confirmation or a large query needs more time. An MCP timeout is not proof that a Dataverse operation was cancelled: use the tool's Cancel button and inspect the outcome before retrying a modification. Bulk delete jobs continue asynchronously once submitted.

### Query Execution

Write your query in the main editor, and press `F5` or `Ctrl+E` / `Cmd+E` to run it. With no text selected, all the text in the current editor will be executed. To run only
a specific section, highlight the command(s) to run first.

If you are executing a command that will change data, a confirmation prompt will appear first. You can adjust the thresholds at which these confirmations appear in the settings.
By default, all `DELETE` and `UPDATE` statements will produce a confirmation, as will any `INSERT` statement that inserts more than 1 record. If you are running a batch of
multiple statements you can accept the change for an individual statement by clicking "Yes", or accept all remaining changes from other statements in the batch as well by clicking
"Yes To All". Any `DELETE` or `UPDATE` statements without a corresponding `WHERE` clause will be blocked by default, allow this in the settings if necessary or add a `WHERE 1=1`
clause to allow a query to intentionally affect all records in the table.

The results will be shown at the bottom of the screen. By default, any data will be shown in a grid view. If your query produces multiple resultsets, each one will be shown
in a separate grid - scroll down to review all your results. If desired, switch the output to text to have a simple method to copy & paste all the output.
Your most recent choice is remembered for new tabs and the next time you open the tool.

The "Messages" tab will contain any additional status messages that are produced by your query. This is also where you can find any error details, although these will also
be highlighted with red wavy underlining for the problem section of your query.

In the results grid, right-click to get options to export the results in a variety of formats. Any values which are links to other records (from Lookup or primary key
type columns) will be shown as links - click these to open the record they refer to in your web browser, or right-click them for options to copy the URL for the record or
create a `SELECT` statement to get more details.

### What this tool changes

SQL 4 CDS can be used to change data in any writeable table in your environment, please use it with care!

### Connections

SQL 4 CDS supports multiple connections. Each query tab is attached to one primary connection which can be changed using the drop-down list in the toolbar, but each query
can work with data from multiple connections. To reference a table in a different connection, use the syntax `{connection name}.{schema}.{table}`, e.g.:

```sql
SELECT prod.fullname, dev.fullname
FROM [My Prod Connection].dbo.systemuser AS prod
FULL OUTER JOIN [My Dev Connection].dbo.systemuser AS dev
ON prod.domainname = dev.domainname
```

Add connections to the tool by clicking on the connection slot numbers at the bottom of the PPTB window.

### Object Explorer

The Object Explorer is shown on the left of the SQL 4 CDS window and displays all the current connections. Expand each connection to show the tables, columns and other objects
you can use in your queries.

Double-click on a table, column, stored procedure or function name to insert it into your current query.

### Execution Plans

You can retrieve an estimated execution plan in advance of running a query, or opt to view the actual plan used as part of query execution. Both plans will be the same, although
the actual plan will also include statistics such as how long each node took to execute, number of rows it produced etc.

From the execution plan view, select a node and open the "Properties" tool window on the right hand side to view more information about it.

Double-click on a "FetchXML Scan" node to open the FetchXML query in a new tab. From there you can also click the toolbar button to open the query in FetchXML Studio for
further editing.

You can also convert a query directly to FetchXML using the "Convert" toolbar button. Click the dropdown arrow next to this to convert the query to an M query ready to be added
directly into Power BI instead.

## Error telemetry

The **Include detailed error telemetry** option in SQL 4 CDS Settings sends SQL query text and full exception details, including stack traces,
to Microsoft Application Insights when an error occurs. These details may contain sensitive data. The option is off by default; changes apply
to existing connections without restarting the tool. When disabled, error telemetry excludes SQL text and full exception details.

## Limitations

The following are not available when running SQL 4 CDS inside PPTB compared to other implementations such as XrmToolBox:

- TDS endpoint is not supported. All queries will use FetchXML or other methods to retrieve the required data instead.
- Party-list updates (such as `to` and `from` on activities) retrieve the existing party-list values and include unchanged
  roles in the activity's combined Web API payload. This may cause plugins to be triggered differently to when the same
  `UPDATE` query is executed via the XrmToolBox version.

## AI Assistance

Parts of this tool were generated with GitHub Copilot and reviewed, tested, and maintained by the contributors listed in package.json.
