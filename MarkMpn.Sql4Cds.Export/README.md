A fork of various classes from https://github.com/microsoft/sqltoolsservice to handle storing and exporting
data from a data reader to a file in various different formats. Extended to handle SqlEntityReference values
as hyperlinks in Excel and Markdown exports.

Excel column auto-sizing uses an approximate seven-pixel character width for 11-point Calibri,
with extra space for bold headers and filter buttons. Multiline values use the longest line,
tabs count as four characters, and column widths are capped at Excel's 255-character limit.
This avoids native font-rendering dependencies; widths are estimates rather than exact font measurements.