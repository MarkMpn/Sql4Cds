using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExpressionTests
    {
        [TestMethod]
        public void SimpleCaseExpression()
        {
            var expr = new SimpleCaseExpression
            {
                InputExpression = Col("name"),
                WhenClauses =
                {
                    new SimpleWhenClause
                    {
                        WhenExpression = Str("one"),
                        ThenExpression = Int(1)
                    },
                    new SimpleWhenClause
                    {
                        WhenExpression = Str("two"),
                        ThenExpression = Int(2)
                    }
                },
                ElseExpression = Int(3)
            };
            var schema = new NodeSchema(new Dictionary<string, DataTypeReference>
            {
                ["name"] = DataTypeHelpers.NVarChar(100)
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>(), Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var func = expr.Compile(schema, parameterTypes);

            var options = new StubOptions();
            var parameterValues = new Dictionary<string, object>();

            var record = new Entity
            {
                ["name"] = SqlTypeConverter.UseDefaultCollation("One")
            };

            var value = func(record, parameterValues, options);
            Assert.AreEqual((SqlInt32)1, value);

            record["name"] = SqlTypeConverter.UseDefaultCollation("Two");
            value = func(record, parameterValues, options);
            Assert.AreEqual((SqlInt32)2, value);

            record["name"] = SqlTypeConverter.UseDefaultCollation("Five");
            value = func(record, parameterValues, options);
            Assert.AreEqual((SqlInt32)3, value);
        }

        [TestMethod]
        public void FormatDateTime()
        {
            var expr = new FunctionCall
            {
                FunctionName = new Identifier { Value = "FORMAT" },
                Parameters =
                {
                    Col("createdon"),
                    Str("yyyy-MM-dd")
                }
            };
            var schema = new NodeSchema(new Dictionary<string, DataTypeReference>
            {
                ["createdon"] = DataTypeHelpers.DateTime
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>(), Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var func = expr.Compile(schema, parameterTypes);

            var options = new StubOptions();
            var parameterValues = new Dictionary<string, object>();

            var record = new Entity
            {
                ["createdon"] = (SqlDateTime)new DateTime(2022, 1, 2)
            };

            var value = func(record, parameterValues, options);
            Assert.AreEqual(SqlTypeConverter.UseDefaultCollation("2022-01-02"), value);
        }

        private ColumnReferenceExpression Col(string name)
        {
            return new ColumnReferenceExpression
            {
                MultiPartIdentifier = new MultiPartIdentifier
                {
                    Identifiers =
                    {
                        new Identifier
                        {
                            Value = name
                        }
                    }
                }
            };
        }

        private StringLiteral Str(string value)
        {
            return new StringLiteral { Value = value };
        }

        private IntegerLiteral Int(int value)
        {
            return new IntegerLiteral { Value = value.ToString(CultureInfo.InvariantCulture) };
        }
    }
}
