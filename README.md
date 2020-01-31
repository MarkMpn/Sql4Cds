# ![](https://markcarrington.dev/sql4cds-icon/) SQL 4 CDS

By [Mark Carrington](https://markcarrington.dev/sql-4-cds/), supported by [Data8](https://www.data-8.co.uk/)

SQL 4 CDS provides an [engine](https://www.nuget.org/packages/MarkMpn.Sql4Cds.Engine/) and
[XrmToolBox tool](https://www.xrmtoolbox.com/plugins/MarkMpn.SQL4CDS/) for using standard SQL syntax to query data stored in CDS / Microsoft
Dynamics 365.

It converts the provided SQL query into the corresponding [FetchXML](https://docs.microsoft.com/en-us/powerapps/developer/common-data-service/fetchxml-schema)
syntax and allows the associated query to be executed, including the following types of query:

* `SELECT`
* `INSERT`
* `UPDATE`
* `DELETE`

The engine converts all the SQL syntax that has a direct equivalent in FetchXML. It will also use some other options to improve the execution of specific
queries, including:

* Faster `SELECT count(*) FROM entity` query execution using [RetrieveTotalRecordCountRequest](https://docs.microsoft.com/en-us/dotnet/api/microsoft.crm.sdk.messages.retrievetotalrecordcountrequest?view=dynamics-general-ce-9)
* Work around `AggregateQueryRecordLimit` errors by retrieving all the individual records and applying the aggregation in-memory.