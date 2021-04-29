# ![](https://markcarrington.dev/sql4cds-icon/) SQL 4 CDS

By [Mark Carrington](https://markcarrington.dev/sql-4-cds/), supported by [Data8](https://www.data-8.co.uk/)

SQL 4 CDS provides an [engine](https://www.nuget.org/packages/MarkMpn.Sql4Cds.Engine/) and
[XrmToolBox tool](https://www.xrmtoolbox.com/plugins/MarkMpn.SQL4CDS/) for using standard SQL syntax to query data stored in Microsoft
Dataverse / Dynamics 365.

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
