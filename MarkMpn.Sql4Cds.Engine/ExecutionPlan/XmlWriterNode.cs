using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.IO;
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
                        throw new NotSupportedQueryFragmentException($"Column name '{col.OutputColumn}' is repeated. The same attribute cannot be generated more than once on the same XML tag.");
                }
            }

            Source = Source.FoldQuery(context, hints);
            return this;
        }

        public override INodeSchema GetSchema(NodeCompilationContext context)
        {
            return new NodeSchema(
                schema: new Dictionary<string, DataTypeReference>
                {
                    ["xml"] = XmlType ? (DataTypeReference) DataTypeHelpers.Xml : DataTypeHelpers.NVarChar(8000, context.PrimaryDataSource.DefaultCollation, CollationLabel.CoercibleDefault)
                },
                aliases: null,
                primaryKey: null,
                notNullColumns: null,
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
                    WriteRootElement(RootName, writer);

                foreach (var row in Source.Execute(context))
                {
                    switch (XmlFormat)
                    {
                        case XmlFormat.Raw:
                            WriteRawFormat(row, writer);
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
                value = new SqlXml(XmlReader.Create(new StringReader(buf.ToString())));

            yield return new Entity
            {
                ["xml"] = value
            };
        }

        private void WriteRootElement(string rootName, XmlWriter writer)
        {
            writer.WriteStartElement(rootName);

            if (ColumnFormat.HasFlag(XmlColumnFormat.XsiNil))
                writer.WriteAttributeString("xmlns", "xsi", null, XsiNamespace);
        }

        private void WriteRawFormat(Entity row, XmlWriter writer)
        {
            if (String.IsNullOrEmpty(ElementName))
                throw new InvalidOperationException("ElementName must be specified when using Raw format");

            writer.WriteStartElement(ElementName);

            foreach (var col in ColumnSet)
            {
                var value = (INullable) row[col.SourceColumn];

                if (ColumnFormat.HasFlag(XmlColumnFormat.Element) && (!value.IsNull || ColumnFormat.HasFlag(XmlColumnFormat.XsiNil)))
                {
                    writer.WriteStartElement(col.OutputColumn);

                    if (value.IsNull)
                        writer.WriteAttributeString("nil", XsiNamespace, "true");

                    writer.WriteEndElement();
                }
                else if (ColumnFormat == XmlColumnFormat.Attribute && !value.IsNull)
                {
                    writer.WriteAttributeString(col.OutputColumn, value.ToString());
                }
            }

            writer.WriteEndElement();
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
