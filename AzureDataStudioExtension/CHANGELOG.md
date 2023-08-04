# Change Log

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