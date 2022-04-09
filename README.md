# ![](https://markcarrington.dev/sql4cds-icon/) SQL 4 CDS

By [Mark Carrington](https://markcarrington.dev/sql-4-cds/), supported by [Data8](https://www.data-8.co.uk/)

SQL 4 CDS provides an [engine](https://www.nuget.org/packages/MarkMpn.Sql4Cds.Engine/),
[XrmToolBox tool](https://www.xrmtoolbox.com/plugins/MarkMpn.SQL4CDS/) and [SSMS plugin](https://markcarrington.dev/sql-4-cds/sql-4-cds-ssms-edition/)
for using standard SQL syntax to query data stored in Microsoft Dataverse / Dynamics 365.

It converts the provided SQL query into the corresponding [FetchXML](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/fetchxml-schema?WT.mc_id=DX-MVP-5004203)
syntax and allows the associated query to be executed, including the following types of query:

* `SELECT`
* `INSERT`
* `UPDATE`
* `DELETE`

For example:

```sql
-- Get contact details
SELECT   c.firstname,
         c.lastname,
         a.telephone1
FROM     contact AS c
         INNER JOIN account AS a
         ON c.parentcustomerid = a.accountid
WHERE    c.firstname = 'Mark' AND
         a.statecode = 0
ORDER BY c.createdon DESC

-- Deactivate contacts without an email address
UPDATE contact
SET    statecode = 1, statuscode = 2
WHERE  emailaddress1 IS NULL
```

> ✅ Although you are writing SQL, you are not directly running the queries against the back-end database. All data retrieval and
> modification is done through the supported Dataverse API. Running an UPDATE/INSERT/DELETE command against the underlying SQL
> database is unsafe, but the same query in SQL 4 CDS is translated to safe & supported API requests.

The engine converts all the SQL syntax that has a direct equivalent in FetchXML. It also attempts to support some more SQL features
that do not have an equivalent in FetchXML, such as calculated fields, `HAVING` clauses and more.

When executing a query it will take into account specific Dataverse features to improve the performance or results compared to
simply executing the FetchXML directly, e.g.:

* Faster `SELECT count(*) FROM entity` query execution using [RetrieveTotalRecordCountRequest](https://docs.microsoft.com/dotnet/api/microsoft.crm.sdk.messages.retrievetotalrecordcountrequest?WT.mc_id=DX-MVP-5004203)
* Automatically retrieving multiple pages of large result sets
* Work around `AggregateQueryRecordLimit` errors by retrieving all the individual records and applying the aggregation in-memory.

As well as querying data with FetchXML, SQL 4 CDS can also query metadata by translating the SQL query into a
[RetrieveMetadataChangesRequest](https://docs.microsoft.com/dotnet/api/microsoft.xrm.sdk.messages.retrievemetadatachangesrequest?WT.mc_id=DX-MVP-5004203) or 
[RetrieveAllOptionSetsRequest](https://docs.microsoft.com/dotnet/api/microsoft.xrm.sdk.messages.retrievealloptionsetsrequest?WT.mc_id=DX-MVP-5004203):

```sql
-- Find attributes without a description
SELECT entity.logicalname,
       attribute.logicalname
FROM   metadata.entity
       INNER JOIN metadata.attribute
       ON entity.logicalname = attribute.entitylogicalname
WHERE  attribute.description IS NULL
```

## FetchXML Builder Integration

As well as writing and executing queries as SQL, the generated FetchXML can be sent to [FetchXML Builder](https://fetchxmlbuilder.com/)
for further editing or converting to another syntax such as OData. You can also start building a query in FetchXML Builder and then edit
it in SQL 4 CDS.

## Library Usage

The NuGet package includes assemblies for .NET Framework 4.6.2 and later, and .NET Core 3.1 and later.

You can use it in the same way as a SQL Server connection, using `Sql4CdsConnection` instead of `SqlConnection`:

```csharp
using (var con = new Sql4CdsConnection(connectionString))
using (var cmd = con.CreateCommand())
{
	cmd.CommandText = "INSERT INTO account (name) VALUES (@name)";

	var nameParam = cmd.CreateParameter();
	nameParam.Name = "@name";
	nameParam.Value =  "My New Account";
	cmd.Parameters.Add(nameParam);

	// Add the new account
	cmd.ExecuteNonQuery();

	// Get the IDs of all accounts with the same name
	cmd.CommandText = "SELECT accountid FROM account WHERE name = @name";

	using (var reader = cmd.ExecuteReader())
	{
		while (reader.Read())
		{
			var accountId = reader.GetGuid(0);
			Console.WriteLine(accountId.ToString());
		}
	}
}
```

The connection string should be any [standard XRM connection string](https://docs.microsoft.com/en-us/powerapps/developer/data-platform/xrm-tooling/use-connection-strings-xrm-tooling-connect?WT.mc_id=DX-MVP-5004203).

If you already have an `IOrganizationService` connection to the instance you want to use, you can pass that to the
constructor instead of the connection string.

You can also connect to multiple instances at once and execute queries to combine or copy data between them. To use this,
pass all the `IOrganizationService` instances to the `Sql4CdsConnection` constructor. You can then reference
data from a specific instance using `instancename.dbo.tablename`.

### Advanced Options

There are various properties available on the `Sql4CdsConnection` class that you can use to control exactly how your queries are executed:

| Property | Description |
|--|--|
| `BatchSize` | When executing DML operations, how many requests should be sent to the server at once? |
| `BypassCustomPlugins` | When executing DML operations, should custom plugins be bypassed? |
| `UseLocalTimeZone` | When working with date values, this property indicates whether the local or UTC time zone should be used. |
| `MaxDegreeOfParallelism` | How many requests can be made in parallel? Currently used for DML and partitioned aggregate queries. |
| `UseTDSEndpoint` | Indicates if the preview TDS Endpoint should be used where possible to execute SELECT queries. |
| `BlockDeleteWithoutWhere` | Indicates if an error should be produced if running a DELETE query without a corresponding WHERE clause. |
| `BlockUpdateWithoutWhere` | Indicates if an error should be produced if running a UPDATE query without a corresponding WHERE clause. |
| `UseBulkDelete` | Set to `true` to use a bulk delete job instead of deleting individual records for a DELETE query. |
| `ReturnEntityReferenceAsGuid` | Indicates if lookup values should be returned as simple `Guid` values rather than the default `SqlEntityReference` type. |
| `UseRetrieveTotalRecordCount` | Indicates if a [RetrieveTotalRecordCountRequest](https://docs.microsoft.com/dotnet/api/microsoft.crm.sdk.messages.retrievetotalrecordcountrequest?WT.mc_id=DX-MVP-5004203) request should be used for simple `COUNT(*)` queries. This lets the query run faster but may produce out-of-date results. |
| `QuotedIdentifiers` | Indicates if `"` can be used to quote identifiers such as column and table names. Equivalent to `SET QUOTED_IDENTIFIERS ON`. |

There are also events that you can attach to to receive notifications while a query is executing. The `InfoMessage` and `StatementCompleted` events follow the pattern
provided by the SqlClient classes for SQL Server, but add extra data specific to SQL 4 CDS.

| Event | Description |
|--|--|
| `PreDelete`<br />`PreInsert`<br />`PreUpdate` | These events on the connection are raised just before an INSERT/DELETE/UPDATE command is about to be executed. The event argument includes the metadata of the entity type that will be affected along with the number of rows. The event handler can prevent the operation by setting the `Cancel` property of the event argument to `true`. Cancelling the operation will also cancel the entire batch. |
| `PreRetrieve` | This event on the connection is raised just before more data is about to be retrieved from the server. The event argument contains the number of rows already retrieved so far. The event handler can prevent the retrieval from continuing by setting the `Cancel` property of the event argument to `true`. Cancelling a data retrieval will not cancel the entire batch, but will cause it to operate only on partial results. |
| `Progress` | This event on the connection is raised when there is some update to the internal progress of executing a query, and can be used to provide feedback to the user that their query is progressing. |
| `InfoMessage` | This event on the connection is raised when there is some textual output from the query available. |
| `StatementCompleted` | This event on the command is raised when a statement within the current query has completed successfully. The event arguments show the number of records that were affected by the query as well as the details of the internal query plan that was executed for the statement. |