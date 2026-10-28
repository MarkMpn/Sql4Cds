# SQL 4 CDS for Power Platform ToolBox

Query and modify Dataverse data using standard SQL syntax inside [Power Platform ToolBox](https://www.powerplatformtoolbox.com/) (PPTB).

All standard table data is available to query using `SELECT` statements. Data can also be modified using `INSERT`, `UPDATE` and `DELETE` statements.

## Features

A wide variety of SQL Server compatible T-SQL syntax is supported, see the [SQL 4 CDS wiki](https://github.com/MarkMpn/Sql4Cds/wiki) for details.

### Query Editing

A variety of tools are available to help you write queries quickly and accurately.

As you type a query, autocomplete dropdowns may appear to show possible table names, column names and other snippets. Press `Ctrl+Space` to open it at any point.

Hover the mouse over a table, column, function or stored procedure name to view a tooltip with more details.

Queries can be saved and opened to allow you to reuse them. If you close SQL 4 CDS while you have unsaved changes, you'll be prompted to cancel the closure to give you
chance to save them, though you can always click "Ingore & Close" to close the tool anyway without saving.

To help keep queries consistent, use the "Format" toolbar button. Your query will be adjusted with standard line breaks, spacing and other rules enforced, but the structure
of the query itself will remain unchanged.

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

The "Messages" tab will contain any additional status messages that are produced by your query. This is also where you can find any error details, although these will also
be highlighted with red wavy underlining for the problem section of your query.

In the results grid, right-click to get options to export the results in a variety of formats. Any values which are links to other records (from Lookup or primary key
type columns) will be shown as links - click these to open the record they refer to in your web browser, or right-click them for options to copy the URL for the record or
create a `SELECT` statement to get more details.

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

## Limitations

The following are not available when running SQL 4 CDS inside PPTB compared to other implementations such as XrmToolBox:

- TDS endpoint is not supported. All queries will use FetchXML or other methods to retrieve the required data instead.
- Party-list updates (such as `to` and `from` on activities) retrieve the existing party-list values and include unchanged
  roles in the activity's combined Web API payload. This may cause plugins to be triggered differently to when the same
  `UPDATE` query is executed via the XrmToolBox version.
