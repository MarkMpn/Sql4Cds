using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Text;
using System.Xml;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.ExecutionPlan
{
    class XmlWriterNode : BaseDataNode, ISingleSourceExecutionPlanNode
    {
        private const string XsiNamespace = "http://www.w3.org/2001/XMLSchema-instance";

        /// <summary>
        /// The format of the XML to write
        /// </summary>
        [Category("XML Writer")]
        [Description("The format of the XML to write")]
        [DisplayName("XML Format")]
        public XmlFormat XmlFormat { get; set; }

        /// <summary>
        /// The main element name to use when writing XML
        /// </summary>
        [Category("XML Writer")]
        [Description("The main element name to use when writing XML")]
        [DisplayName("Element Name")]
        public string ElementName { get; set; }

        /// <summary>
        /// Indicates whether the result should be returned as a string or XML type
        /// </summary>
        [Category("XML Writer")]
        [Description("Indicates whether the result should be returned as a string or XML type")]
        [DisplayName("XML Type")]
        public bool XmlType { get; set; }

        /// <summary>
        /// The name of the root element to use when writing XML
        /// </summary>
        [Category("XML Writer")]
        [Description("The name of the root element to use when writing XML")]
        [DisplayName("Root Name")]
        public string RootName { get; set; }

        /// <summary>
        /// Describes how columns should be formatted in the XML
        /// </summary>
        [Category("XML Writer")]
        [Description("Describes how columns should be formatted in the XML")]
        [DisplayName("Column Format")]
        public XmlColumnFormat ColumnFormat { get; set; }

        /// <summary>
        /// The columns that should be included in the query results
        /// </summary>
        [Category("XML Writer")]
        [Description("The columns that should be included in the query results")]
        [DisplayName("Column Set")]
        public List<SelectColumn> ColumnSet { get; } = new List<SelectColumn>();

        /// <summary>
        /// The data source to select from
        /// </summary>
        [Browsable(false)]
        public IDataExecutionPlanNodeInternal Source { get; set; }

        public override void AddRequiredColumns(NodeCompilationContext context, IList<string> requiredColumns)
        {
            foreach (var col in ColumnSet.Select(c => c.SourceColumn))
            {
                if (!requiredColumns.Contains(col, StringComparer.OrdinalIgnoreCase))
                    requiredColumns.Add(col);
            }

            Source.AddRequiredColumns(context, requiredColumns);
        }

        public override IDataExecutionPlanNodeInternal FoldQuery(NodeCompilationContext context, IList<OptimizerHint> hints)
        {
            // If columns are output as attributes, each column must be unique
            if (ColumnFormat.HasFlag(XmlColumnFormat.Attribute))
            {
                var seen = new HashSet<string>();
                for (var i = 0; i < ColumnSet.Count; i++)
                {
                    var col = ColumnSet[i];
                    if (!seen.Add(col.OutputColumn))
                        throw new NotSupportedQueryFragmentException($"Column name '{col.OutputColumn}' is repeated. The same attribute cannot be generated more than once on the same XML tag");
                }
            }

            if (XmlFormat == XmlFormat.Raw)
            {
                // Can't use empty row element in attribute-centric XML
                if (String.IsNullOrEmpty(ElementName) && ColumnFormat.HasFlag(XmlColumnFormat.Attribute))
                    throw new NotSupportedQueryFragmentException("Row tag omission (empty row tag name) cannot be used with attribute-centric FOR XML serialization");
            }
            else if (XmlFormat == XmlFormat.Path)
            {
                // Validate the sequence of columns produces valid XML
                string lastElement = null;

                foreach (var col in ColumnSet)
                {
                    // Can't have an attribute column with no row tag
                    if (col.OutputColumn?.StartsWith("@") == true && String.IsNullOrEmpty(ElementName))
                        throw new NotSupportedQueryFragmentException("Row tag omission (empty row tag name) cannot be used with attribute-centric FOR XML serialization");

                    if (!String.IsNullOrEmpty(col.OutputColumn) && col.OutputColumn != "*")
                    {
                        var parts = col.OutputColumn.Split('/');

                        for (var i = 0; i < parts.Length; i++)
                        {
                            // Attributes and node tests can't have child elements
                            if (parts[i].StartsWith("@") || parts[i] == "text()" || parts[i] == "comment()" || parts[i] == "node()" || parts[i].StartsWith("processing-instruction("))
                            {
                                if (i < parts.Length - 1)
                                    throw new NotSupportedQueryFragmentException($"Column name '{col.OutputColumn}' is invalid. Attributes and node tests cannot have child elements");

                                if (parts[i].StartsWith("@"))
                                {
                                    // Attribute names must be valid XML identifiers
                                    try
                                    {
                                        XmlConvert.VerifyName(parts[i].Substring(1));
                                    }
                                    catch
                                    {
                                        throw new NotSupportedQueryFragmentException($"Column name '{col.OutputColumn}' contains an invalid XML identifier as required by FOR XML");
                                    }
                                }
                            }
                            else
                            {
                                // Element names must be valid XML identifiers
                                try
                                {
                                    XmlConvert.VerifyName(parts[i]);
                                }
                                catch
                                {
                                    throw new NotSupportedQueryFragmentException($"Column name '{col.OutputColumn}' contains an invalid XML identifier as required by FOR XML");
                                }
                            }
                        }

                        var elementName = ElementName + "/" + col.OutputColumn;

                        if (parts[parts.Length - 1].StartsWith("@") ||
                            parts[parts.Length - 1] == "text()" ||
                            parts[parts.Length - 1] == "comment()" ||
                            parts[parts.Length - 1] == "node()" ||
                            parts[parts.Length - 1] == "data()" ||
                            parts[parts.Length - 1].StartsWith("processing-instruction("))
                        {
                            // Attribute or node test
                            elementName = ElementName + "/" + String.Join("/", parts, 0, parts.Length - 1);

                            if (parts[parts.Length - 1].StartsWith("@"))
                            {
                                // Can't add attributes to elements with content
                                if (lastElement == elementName)
                                    throw new NotSupportedQueryFragmentException($"Attribute-centric column '{col.OutputColumn}' must not come after a non-attribute-centric sibling in XML hierarchy in FOR XML PATH");
                            }
                        }
                        else
                        {
                            lastElement = elementName;
                        }
                    }
                    else if (String.IsNullOrEmpty(lastElement))
                    {
                        // Columns without a name or named "*" are inlined
                        // https://learn.microsoft.com/en-us/sql/relational-databases/xml/columns-without-a-name?view=sql-server-ver16
                        // https://learn.microsoft.com/en-us/sql/relational-databases/xml/columns-with-a-name-specified-as-a-wildcard-character?view=sql-server-ver16
                        lastElement = ElementName;
                    }
                }
            }

            Source = Source.FoldQuery(context, hints);
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return new NodeSchema(
                schema: new ColumnList
                {
                    ["xml"] = new ColumnDefinition(XmlType ? (DataTypeReference) DataTypeHelpers.Xml : DataTypeHelpers.NVarChar(Int32.MaxValue, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault), true, true)
                },
                aliases: null,
                primaryKey: null,
                sortOrder: null
                );
        }

        public override IEnumerable<IExecutionPlanNode> GetSources()
        {
            yield return Source;
        }

        protected override RowCountEstimate EstimateRowsOutInternal(NodeCompilationContext context)
        {
            return RowCountEstimateDefiniteRange.ExactlyOne;
        }

        protected override IEnumerable<Entity> ExecuteInternal(NodeExecutionContext context)
        {
            var buf = new StringBuilder();
            
            using (var writer = XmlWriter.Create(buf, new XmlWriterSettings { ConformanceLevel = ConformanceLevel.Fragment }))
            {
                if (!String.IsNullOrEmpty(RootName))
                    WriteStartElement(RootName, writer);

                foreach (var row in Source.Execute(context))
                {
                    switch (XmlFormat)
                    {
                        case XmlFormat.Raw:
                            WriteRawFormat(row, writer);
                            break;

                        case XmlFormat.Path:
                            WritePathFormat(row, writer);
                            break;

                        default:
                            throw new NotImplementedException();
                    }
                }

                if (!String.IsNullOrEmpty(RootName))
                    writer.WriteEndElement();
            }

            object value;

            if (!XmlType)
                value = context.PrimaryDataSource.DefaultCollation.ToSqlString(buf.ToString());
            else
                value = new SqlXml(XmlReader.Create(new StringReader(buf.ToString()), new XmlReaderSettings { ConformanceLevel = ConformanceLevel.Fragment }));

            yield return new Entity
            {
                ["xml"] = value
            };
        }

        private void WriteStartElement(string elementName, XmlWriter writer)
        {
            var isRoot = writer.WriteState == WriteState.Start;

            writer.WriteStartElement(elementName);

            if (ColumnFormat.HasFlag(XmlColumnFormat.XsiNil) && isRoot)
                writer.WriteAttributeString("xmlns", "xsi", null, XsiNamespace);
        }

        private void WriteRawFormat(Entity row, XmlWriter writer)
        {
            if (!String.IsNullOrEmpty(ElementName))
                WriteStartElement(ElementName, writer);

            foreach (var col in ColumnSet)
            {
                var value = (INullable) row[col.SourceColumn];

                if (ColumnFormat.HasFlag(XmlColumnFormat.Element) && (!value.IsNull || ColumnFormat.HasFlag(XmlColumnFormat.XsiNil)))
                {
                    WriteStartElement(col.OutputColumn, writer);

                    if (value.IsNull)
                        writer.WriteAttributeString("nil", XsiNamespace, "true");

                    writer.WriteEndElement();
                }
                else if (ColumnFormat == XmlColumnFormat.Attribute && !value.IsNull)
                {
                    writer.WriteAttributeString(col.OutputColumn, value.ToString());
                }
            }

            if (!String.IsNullOrEmpty(ElementName))
                writer.WriteEndElement();
        }

        private void WritePathFormat(Entity row, XmlWriter writer)
        {
            if (!String.IsNullOrEmpty(ElementName))
                WriteStartElement(ElementName, writer);

            var elements = new Stack<string>();
            var lastNull = false;
            var inDataList = false;

            foreach (var col in ColumnSet)
            {
                var value = (INullable)row[col.SourceColumn];
                var parts = col.OutputColumn?.Split('/') ?? new[] { "" };

                EnsureElements(parts, elements, writer, ref lastNull, out var modified);

                var name = parts[parts.Length - 1];

                if (String.IsNullOrEmpty(name) || name == "*" || name == "text()")
                {
                    if (!value.IsNull)
                    {
                        writer.WriteString(value.ToString());
                        lastNull = false;
                    }
                }
                else if (name == "data()")
                {
                    if (modified)
                        inDataList = false;

                    if (!value.IsNull)
                    {
                        if (inDataList)
                            writer.WriteString(" ");

                        writer.WriteString(value.ToString());
                        inDataList = true;
                        lastNull = false;
                    }
                }
                else if (name.StartsWith("@"))
                {
                    if (!value.IsNull)
                        writer.WriteAttributeString(name.Substring(1), value.ToString());
                }
                else
                {
                    if (!value.IsNull || ColumnFormat.HasFlag(XmlColumnFormat.XsiNil))
                    {
                        elements.Push(name);
                        WriteStartElement(name, writer);

                        if (!value.IsNull)
                            writer.WriteString(value.ToString());
                        else
                            lastNull = true;
                    }
                }

                if (name != "data()")
                    inDataList = false;
            }

            while (elements.Count > 0)
            {
                if (lastNull)
                {
                    writer.WriteAttributeString("nil", XsiNamespace, "true");
                    lastNull = false;
                }

                writer.WriteEndElement();
                elements.Pop();
            }

            if (!String.IsNullOrEmpty(ElementName))
                writer.WriteEndElement();
        }

        private void EnsureElements(string[] parts, Stack<string> elements, XmlWriter writer, ref bool lastNull, out bool modified)
        {
            modified = false;

            // Close any additional elements
            while (elements.Count > parts.Length - 1)
            {
                if (lastNull)
                {
                    writer.WriteAttributeString("nil", XsiNamespace, "true");
                    lastNull = false;
                }

                elements.Pop();
                writer.WriteEndElement();
                modified = true;
            }

            // Check the remaining elements
            var i = 0;
            foreach (var element in elements.Reverse())
            {
                if (element == parts[i])
                {
                    i++;
                    continue;
                }

                // Element is different, close it and all subsequent elements
                while (elements.Count > i)
                {
                    elements.Pop();
                    writer.WriteEndElement();
                    lastNull = false;
                    modified = true;
                }

                break;
            }

            // Open any additional elements
            while (elements.Count < parts.Length - 1)
            {
                WriteStartElement(parts[elements.Count], writer);
                elements.Push(parts[elements.Count]);
                lastNull = false;
                modified = true;
            }
        }

        public override object Clone()
        {
            var clone = new XmlWriterNode
            {
                Source = (IDataExecutionPlanNodeInternal)Source.Clone(),
                ColumnFormat = ColumnFormat,
                ElementName = ElementName,
                RootName = RootName,
                XmlFormat = XmlFormat,
                XmlType = XmlType
            };

            clone.Source.Parent = clone;

            foreach (var col in ColumnSet)
                clone.ColumnSet.Add(col);

            return clone;
        }
    }

    enum XmlFormat
    {
        Raw,
        Auto,
        Explicit,
        Path
    }

    [Flags]
    enum XmlColumnFormat
    {
        Attribute = 0,
        Element = 1,
        XsiNil = 2
    }
}
