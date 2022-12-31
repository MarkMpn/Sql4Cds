# SQL 4 CDS extension for Azure Data Studio

> **Donations**
>
> SQL 4 CDS is free, but please consider [donating] if it has helped you!

Query and modify data in Microsoft Dataverse / Dynamics 365 / CRM instances using standard SQL syntax in Azure Data Studo. This extension enables you to use Azure Data Studio features with Dataverse like:

* Connect to online and on-premise instances of Dataverse/CRM
* Use the object explorer view and auto-completion to find tables and columns to use in your queries
* View query results using built-in tables and charting tools
* Dashboards to explore server & database settings
* Save and group connections for easy access
* View execution plan details to convert SQL queries to FetchXML

![Connection Dialog]

## Install the SQL 4 CDS extension

Choose the `Install from VSIX...` option in the Extension view and installing a bundled release from the [Releases] page.
You will need to have [.NET 7] already installed on your machine.

## About SQL 4 CDS

This extension uses the core SQL 4 CDS engine which converts SQL syntax queries to the corresponding FetchXML queries or
Insert/Update/Delete requests to modify data. It does not make any unsupported access to the underlying SQL database.

If you have the [TDS Endpoint] enabled, SQL 4 CDS can also make use of that to execute queries where possible.

[Releases]: https://github.com/MarkMpn/Sql4Cds/releases
[donating]: https://www.paypal.com/donate/?cmd=_donations&business=donate@markcarrington.dev&lc=EN&item_name=SQL+4+CDS+Azure+Data+Studio+Donation&currency_code=GBP&bn=PP%2dDonationsBF
[Connection Dialog]:https://user-images.githubusercontent.com/31017244/210132861-11bd29a8-0d60-420a-bd4d-9852398fcd17.png
[TDS Endpoint]:https://learn.microsoft.com/en-us/power-apps/developer/data-platform/dataverse-sql-query?WT.mc_id=DX-MVP-5004203