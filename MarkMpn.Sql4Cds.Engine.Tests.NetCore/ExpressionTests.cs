using System.Data.SqlTypes;
using System.Globalization;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests.NetCore
{
    [TestClass]
    public sealed class ExpressionTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        [DataRow("971881cc-c03c-e811-8125-5065f38bd4e1", true)]
        [DataRow("5fc2f54e-6ab6-f111-aaac-000d3ab0e540", true)]
        [DataRow("772caee8-4ffb-4ccd-8bbc-917de9251543", false)]
        [DataRow(null, false)]
        public void GuidInStringList(string value, bool expected)
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/825

            var expr = new InPredicate
            {
                Expression = Col("metadataid"),
                Values =
                {
                    Str("971881cc-c03c-e811-8125-5065f38bd4e1"),
                    Str("5fc2f54e-6ab6-f111-aaac-000d3ab0e540")
                }
            };
            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>
            {
                ["metadataid"] = new ExecutionPlan.ColumnDefinition(DataTypeHelpers.UniqueIdentifier, true, false)
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(new SessionContext(_localDataSources, options), options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var record = new Entity
            {
                ["metadataid"] = value == null ? SqlGuid.Null : SqlGuid.Parse(value)
            };
            var executionContext = new ExpressionExecutionContext(compilationContext);
            executionContext.Entity = record;

            var actual = func(executionContext);
            Assert.AreEqual((SqlBoolean)expected, actual);
        }

        [TestMethod]
        [DataRow("971881cc-c03c-e811-8125-5065f38bd4e1", 1)]
        [DataRow("5fc2f54e-6ab6-f111-aaac-000d3ab0e540", 1)]
        [DataRow("772caee8-4ffb-4ccd-8bbc-917de9251543", 0)]
        [DataRow(null, 0)]
        public void GuidInStringListWithCase(string value, int expected)
        {
            // https://github.com/MarkMpn/Sql4Cds/issues/825

            var expr = new SearchedCaseExpression
            {
                WhenClauses =
                {
                    new SearchedWhenClause
                    {
                        WhenExpression = new InPredicate
                        {
                            Expression = Col("metadataid"),
                            Values =
                            {
                                Str("971881cc-c03c-e811-8125-5065f38bd4e1"),
                                Str("5fc2f54e-6ab6-f111-aaac-000d3ab0e540")
                            }
                        },
                        ThenExpression = Int(1)
                    }
                },
                ElseExpression = Int(0)
            };
            ;
            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>
            {
                ["metadataid"] = new ExecutionPlan.ColumnDefinition(DataTypeHelpers.UniqueIdentifier, true, false)
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(new SessionContext(_localDataSources, options), options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var record = new Entity
            {
                ["metadataid"] = value == null ? SqlGuid.Null : SqlGuid.Parse(value)
            };
            var executionContext = new ExpressionExecutionContext(compilationContext);
            executionContext.Entity = record;

            var actual = func(executionContext);
            Assert.AreEqual((SqlInt32)expected, actual);
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
