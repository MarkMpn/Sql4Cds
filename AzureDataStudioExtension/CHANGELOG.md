# Change Log

## [v9.2.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v9.2.0) - 2024-07-11

Added export to CSV/Excel/JSON/Markdown/XML
Simplify filters that can easily be identified as tautologies or contradictions
Fixed incorrectly matching `null` values in hash joins
Fixed identification of nullable columns in outer joins
Handle `createdon` column being null in some system tables
Fixed use of metadata queries within loops
Fixed filtering and aggregation special cases with `audit` entity
Fixed incorrect type conversion errors when using specialized FetchXML condition operators
Fixed use of `CAST` and `CONVERT` with string types
Fixed paging when using semi-joins
Various fixes when querying virtual entities with unreliable providers:
* values returned as different types
* attributes using names with different case
* not honouring `top`, `offset`, `count`, `order`, `filter`
Improved error reporting:
* when using `*` instead of column name
* when passing incorrect number of parameters to aggregate functions
* when comparing lookup/optionset columns to invalid string values

## [v9.1.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v9.1.0) - 2024-06-10

Enabled access to recycle bin records via the `bin` schema
Enabled `INSERT`, `UPDATE` and `DELETE` statements on `principalobjectaccess` table
Enabled use of subqueries within `ON` clause of `JOIN` statements
Added support for `___pid` virtual column for lookups to elastic tables
Improved folding of queries using index spools
Improved primary key calculation when using joins on non-key columns
Apply column order setting to parameters for stored procedures and table-valued functions
Fixed error with DeleteMultiple requests
Fixed paging error with `DISTINCT` queries causing results to be limited to 50,000 records
Fixed paging errors when sorting by optionset values causing some results to be skipped
Fixed errors when using joins inside `[NOT] EXISTS` subqueries
Fixed incorrect results when applying aliases to `___name` and `___type` virtual columns
Fixed max length calculation for string columns
Fixed display of error messages
Fixed "invalid program" errors when combining type conversions with `AND` or `OR`

## [v9.0.1](https://github.com/MarkMpn/Sql4Cds/releases/tag/v9.0.1) - 2024-05-08

Fixed `NullReferenceException` errors when:
- executing a conditional `SELECT` query
- retrieving results from a Fetch XML query using `IN` or `EXISTS`
- handling an error returned from TDS Endpoint
- handling internal errors such as `UPDATE` without `WHERE`

Standardised errors on:
- JSON path errors
- DML statement cancellation

Fixed filtering `audit` data on `changedata` attribute

## [v9.0.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v9.0.0) - 2024-05-02

Added support for latest Fetch XML features
Support `TRY`, `CATCH` &amp; `THROW` statements and related functions
Error handling consistency with SQL Server
Improved performance with large numbers of expressions and large `VALUES` lists
Generate the execution plan for each statement in a batch only when necessary, to allow initial statements to succeed regardless of errors in later statements
Allow access to catalog views using TDS Endpoint
Inproved `EXECUTE AS` support
Handle missing values in XML `.value()` method
Detect TDS Endpoint incompatibility with XML data type methods and error handling functions
Fixed use of `TOP 1` with `IN` expression
Fixed escaping column names for `SELECT` and `INSERT` commands
Improved setting a partylist attribute based on an EntityReference value
Fixed sorting results for `UNION`
Fold `DISTINCT` to data source for `UNION`
Fold groupings without aggregates to `DISTINCT`
Fixed folding filters through nested loops with outer references
Fixed use of recursive CTE references within subquery
Improved performance of `CROSS APPLY` and `OUTER APPLY`
Improved query cancellation

## [v8.0.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v8.0.0) - 2023-11-25

Added Common Table Expression support:
- Non-recursive CTEs are expanded to subqueries for compatibility with TDS Endpoint
- Recurve CTEs are converted to hierarchical FetchXML filters where possible

Extended JSON support:
- `OPENJSON` table-valued function
- `JSON_QUERY` function
- `ISJSON` function

Query optimizer improvements:
- Prefer hash joins over merge joins if sorts cannot be folded
- Switch FetchXML sorts to custom sorting after adding joins that require custom paging
- Avoid folding filters to tables in subqueries if the same alias exists in the outer query
- Do not use a left outer join to implement `NOT IN` queries where the subquery uses an inner join

Added `IGNORE_DUP_KEY` query hint to ignore duplicate key errors on insert
Added check for multi-currency issues when aggregating non-base currency fields
Added support for disconnecting instances from Azure Data Studio object explorer
Added plugin log messages in error message output
Clearer progress messages for multi-threaded DML operations
Added autocomplete literal value suggestions for entityname attributes

Fixed use of `UNION` with wildcard columns
Fixed error in nested loop joins with no records from inner source
Fixed use of columns from outer queries in join criteria in subqueries
Fixed time zone mismatch when starting bulk delete jobs
Fixed setting polymorphic lookup fields using TDS Endpoint
Fixed aggregates with very dense data distribution

## [v7.6.1](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.6.1) - 2023-10-15

Fixed use of `IN` subqueries when data source cannot be converted to FetchXML
Fixed use of `LIKE` filters with data containing embedded returns
Fixed incorrect row count estimates with joins of huge tables
Fixed left outer join in nested loop when the first record has no matching records from the right source
Fixed use of partitioned aggregates within a loop
Avoid errors when using `DEBUG_BYPASS_OPTIMIZATION` hint
Avoid using custom paging for `IN` and `EXISTS` filters, and where a single child record is guaranteed by the filters

## [v7.6.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.6.0) - 2023-09-30

Allow updating many-to-many intersect tables
Allow folding `NOT EXISTS` predicates to FetchXML for improved performance
Improved sorting reliability on audit and elastic tables
Fixed KeyNotFoundException from certain joins
Fixed NullReferenceException when applying filters to certain joins
Preserve additional join criteria on `metadata` schema tables
Fixed use of `SELECT *` in subqueries

## [v7.5.2](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.5.2) - 2023-09-17

Fixed ordering of hash join results
Improved filter folding across joins
Improved handling of alias names requiring escaping
Improved display of column sets in properties window

## [v7.5.1](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.5.1) - 2023-09-10

Return correct schema for global option set values
Fixed applying alias to primary key field
Fixed CROSS APPLY with UNION ALL and references in each child query to the same outer column
Fixed joins on subqueries with reused table aliases
Autcomplete variable names at start of query

## [v7.5.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.5.0) - 2023-09-03

Added `sql_variant` type support, including `SQL_VARIANT_PROPERTY` function
Added `IS [NOT] DISTINCT FROM` predicate support
Added `@@SERVERNAME` and `@@VERSION` global variables
Added `SERVERPROPERTY` function support
Added `USE_LEGACY_UPDATE_MESSAGES` query hint to use legacy update messages `Assign`, `SetState`, `SetParentBusinessUnit` and `SetBusinessEquipment`

Subquery fixes:
- Only include requested columns from `CROSS APPLY` and query-defined tables
- Performance improvements for uncorrelated scalar subqueries
- Only retrieve minimal rows for scalar subqueries
- Fixed use of nested `IN` and `EXISTS` subqueries

Fixed use of table-valued function with alias
Fixed `JSON_VALUE` function with embedded null literals
Fixed bulk DML operations that require non-standard requests
Do not use merge joins for data types with different sort ordering
Fixed filtering and sorting on non-lowercase attributes
Fixed executing messages with OptionSetValue parameters
Improved error reporting on batch DML statements

Added autocomplete for collation names and variable names

## [v7.4.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.4.0) - 2023-08-05

Added support for long-term retention data
Fixed errors with common XML queries
Improved performance with subqueries
Avoid exposing internal names for computed columns
Convert outer joins with filters to inner joins
Fixed use of `UPDATE` with `CASE` expression with a first value of `NULL`
Fixed use of joins or subqueries with multiple correlated conditions
Improved handling of `LEFT OUTER JOIN` using nested loop operator
Implemented many-to-many joins using merge join operator
Fixed cross-instance string comparison
Lift more filters directly to FetchXML link-entity
Extended folding of sorts to FetchXML around nested loops
Improved ambiguous table name detection in `UPDATE`/`DELETE` statements with subqueries
Avoid errors with un-aliased calculated columns in subqueries
Improved error reporting for `AVG` and `SUM` aggregates on non-numeric types
Handle `MIN` and `MAX` aggregates for primary key and lookup columns
Added support for `STRING_AGG` function
Implemented "Select Top 1000" menu option in Object Explorer
Handle executing queries in workbooks
Fixed filtering metadata by `attribute.sourcetype`

## [v7.3.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.3.0) - 2023-06-12

Added support for `XML` data type and `FOR XML` clause
Added support for Elastic tables
Added support for `STUFF` function
Added option to bypass plugins for `SELECT` statements
Improved error reporting for duplicated table/alias names
Improved error reporting for plugin errors
Improved efficiency of joins that can't be translated to FetchXML
Improved error handling during bulk DML operations
Fixed querying audit table
Fixed collation label for metadata and virtual columns
Fixed hash joins on different collations
Fixed filtering on outer-joined solution table
Fixed multi-threading error with partitioned aggregates
Fixed empty results display

## [v7.2.2](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.2.1) - 2023-05-02

Fixed splitting large INSERT/UPDATE/DELETE requests into batches.

## [v7.2.1](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.2.1) - 2023-04-30

Fixed starting the SQL 4 CDS language server on non-Windows platforms.
Added confirmation prompts and safety limits mirroring the XrmToolBox tool.
Added collation awareness throughout.
Added `PATINDEX`, `UPPER`, `LOWER` and `COLLATIONPROPERTY` functions.
Added `sys.fn_helpcollations()` TVF
Added option to select preferred column order for `SELECT *` queries
Fixed duplicated calculated columns in `SELECT *` queries
Added option to use the recommended degree of parallelism for DML operations.
Added `BATCH_SIZE_n` query hint to control batch size.
Autocomplete improvements handling comments.

## [v7.1.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.1.0) - 2023-01-31

This is the first release of SQL 4 CDS for Azure Data Studio.