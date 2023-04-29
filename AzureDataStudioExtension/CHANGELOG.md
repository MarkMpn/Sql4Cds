# Change Log

## [v7.1.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.1.0) - 2023-01-31

This is the first release of SQL 4 CDS for Azure Data Studio.

## [v7.2.0](https://github.com/MarkMpn/Sql4Cds/releases/tag/v7.2.0) - 2023-04-30

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
