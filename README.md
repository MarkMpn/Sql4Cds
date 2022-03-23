# ![](https://markcarrington.dev/sql4cds-icon/) SQL 4 CDS

By [Mark Carrington](https://markcarrington.dev/sql-4-cds/), supported by [Data8](https://www.data-8.co.uk/)

SQL 4 CDS provides an [engine](https://www.nuget.org/packages/MarkMpn.Sql4Cds.Engine/),
[XrmToolBox tool](https://www.xrmtoolbox.com/plugins/MarkMpn.SQL4CDS/) and [SSMS plugin](https://markcarrington.dev/sql-4-cds/sql-4-cds-ssms-edition/)
for using standard SQL syntax to query data stored in Microsoft Dataverse / Dynamics 365.

It converts the provided SQL query into the corresponding [FetchXML](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/fetchxml-schema)
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

* Faster `SELECT count(*) FROM entity` query execution using [RetrieveTotalRecordCountRequest](https://docs.microsoft.com/dotnet/api/microsoft.crm.sdk.messages.retrievetotalrecordcountrequest)
* Automatically retrieving multiple pages of large result sets
* Work around `AggregateQueryRecordLimit` errors by retrieving all the individual records and applying the aggregation in-memory.

As well as querying data with FetchXML, SQL 4 CDS can also query metadata by translating the SQL query into a
[RetrieveMetadataChangesRequest](https://docs.microsoft.com/dotnet/api/microsoft.xrm.sdk.messages.retrievemetadatachangesrequest) or 
[RetrieveAllOptionSetsRequest](https://docs.microsoft.com/dotnet/api/microsoft.xrm.sdk.messages.retrievealloptionsetsrequest):

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

The main entry point to the library is the `ExecutionPlanBuilder` class. This exposes a `Build()` method
that accepts a SQL string and produces a set of execution plan nodes that the calling application can execute.

The `ExecutionPlanBuilder` class requires details of the data sources (D365 instances) the query will be executed
against, and a set of options that control how the SQL query will be converted and executed.

The `DataSource` class has the following properties:

* `Name` - the name this data source can be referred to by in the SQL query
* `Connection` - the `IOrganizationService` instance that provides access to this data source
* `Metadata` - an `IAttributeMetadataCache` instance that provides cached access to the metadata for this data source.
A standard implementation is provided by `AttributeMetadataCache`
* `TableSizeCache` - an `ITableSizeCache` instance that provides a quick estimate of the number of records in each table
in the data source. A standard implementation is provided by `TableSizeCache`

The calling application must also provide an implementation of the `IQueryExecutionOptions` interface. This provides
various properties and methods that can control how the query is converted and executed:

* `BatchSize` - when executing DML operations, how many requests should be sent to the server at once?
* `BypassCustomPlugins` - when executing DML operations, should custom plugins be bypassed?
* `JoinOperatorsAvailable` - depending on the version of D365, different join operators are available. This property
lists the operators that are available for SQL 4 CDS to use. This should contain `inner` and `outer` at a minimum.
* `UseLocalTimeZone` - when working with date values, this property indicates whether the local or UTC time zone should
be used.
* `ColumnComparisonAvailable` - indicates whether the version of D365 that will be executing the query supports the
FetchXML `valueof` attribute in filter conditions
* `MaxDegreeOfParallelism` - how many requests can be made in parallel? Currently used for DML and partitioned aggregate queries
* `UseTDSEndpoint` - indicates if the preview TDS Endpoint should be used where possible to execute SELECT queries
* `PrimaryEndPoint` - the name of the `DataSource` that queries will run against unless the FROM clause explicitly references
a different data source
* `UserId` - the unique identifier of the current user
* `BlockDeleteWithoutWhere` - indicates if an error should be produced if running a DELETE query without a corresponding WHERE clause
* `BlockUpdateWithoutWhere` - indicates if an error should be produced if running a UPDATE query without a corresponding WHERE clause
* `Cancelled` - set to `true` to stop further execution of the query
* `UseBulkDelete` - set to `true` to use a bulk delete job instead of deleting individual records for a DELETE query

* `ConfirmDelete()` - callback method to allow the application to confirm a DELETE query should go ahead
* `ConfirmInsert()` - callback method to allow the application to confirm an INSERT query should go ahead
* `ConfirmUpdate()` - callback method to allow the application to confirm an UPDATE query should go ahead
* `ConfinueRetrieval()` - callback method to allow the application to stop queries that involve too many data retrieval requests
* `Progress()` - callback method to allow the application to log progress from the query
* `RetrievingNextPagE()` - callback method to notify the application that the query is retrieving another page of data from the server

Once these pieces are in place, the application can execute a query using code such as:

```csharp
var svc = new CrmServiceClient("connectionstring");
var metadata = new AttributeMetadataCache(svc);

var dataSource = new DataSource
{
  Name = "prod",
  Connection = svc,
  Metadata = metadata
  TableSizeCache = new TableSizeCache(svc, metadata)
};

var dataSources = new Dictionary<string, DataSource>
{
	[dataSource.Name] = dataSource
};

var executionPlanBuilder = new ExecutionPlanBuilder(dataSources.Values, options);
var queries = executionPlanBuilder.Build(sql);

foreach (var query in queries)
{
	if (query is IDataSetExecutionPlanNode selectQuery)
	{
		var results = selectQuery.Execute(dataSources, options, null, null);
		// results is a DataTable
		// Display/save/process the results as required
	}
	else if (query is IDmlQueryExecutionPlanNode dmlQuery)
	{
		var message = dmlQuery.Execute(dataSources, options, null, null);
		// message is a description of the affect of executing the INSERT/UPDATE/DELETE query
		// Display/log it as required
	}
}
```
