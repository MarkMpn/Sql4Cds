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
    public class ExpressionTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void StringLiteral()
        {
            var expr = new StringLiteral { Value = "abc" };

            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>(), new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var actual = func(new ExpressionExecutionContext(compilationContext));

            Assert.AreEqual("abc", ((SqlString)actual).Value);
            Assert.AreEqual(typeof(SqlString), expr.GetType(compilationContext, out _));
        }

        [TestMethod]
        public void IntegerLiteral()
        {
            var expr = new IntegerLiteral { Value = "123" };

            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>(), new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var actual = func(new ExpressionExecutionContext(compilationContext));

            Assert.AreEqual(123, ((SqlInt32)actual).Value);
            Assert.AreEqual(typeof(SqlInt32), expr.GetType(compilationContext, out _));
        }

        [TestMethod]
        public void StringConcat()
        {
            var expr = new BinaryExpression
            {
                FirstExpression = new StringLiteral { Value = "hello " },
                BinaryExpressionType = BinaryExpressionType.Add,
                SecondExpression = new StringLiteral { Value = "world" }
            };

            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>(), new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var actual = func(new ExpressionExecutionContext(compilationContext));

            Assert.AreEqual("hello world", ((SqlString)actual).Value);
        }

        [TestMethod]
        public void IntegerAddition()
        {
            var expr = new BinaryExpression
            {
                FirstExpression = new IntegerLiteral { Value = "123" },
                BinaryExpressionType = BinaryExpressionType.Add,
                SecondExpression = new IntegerLiteral { Value = "456" }
            };

            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>(), new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var actual = func(new ExpressionExecutionContext(compilationContext));

            Assert.AreEqual(579, ((SqlInt32)actual).Value);
        }

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
            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>
            {
                ["name"] = new ExecutionPlan.ColumnDefinition(DataTypeHelpers.NVarChar(100, Collation.USEnglish, CollationLabel.CoercibleDefault), true, false)
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var record = new Entity
            {
                ["name"] = compilationContext.PrimaryDataSource.DefaultCollation.ToSqlString("One")
            };
            var executionContext = new ExpressionExecutionContext(compilationContext);
            executionContext.Entity = record;

            var value = func(executionContext);
            Assert.AreEqual((SqlInt32)1, value);

            record["name"] = compilationContext.PrimaryDataSource.DefaultCollation.ToSqlString("Two");
            value = func(executionContext);
            Assert.AreEqual((SqlInt32)2, value);

            record["name"] = compilationContext.PrimaryDataSource.DefaultCollation.ToSqlString("Five");
            value = func(executionContext);
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
            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>
            {
                ["createdon"] = new ExecutionPlan.ColumnDefinition(DataTypeHelpers.DateTime, true, false)
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var record = new Entity
            {
                ["createdon"] = (SqlDateTime)new DateTime(2022, 1, 2)
            };
            var executionContext = new ExpressionExecutionContext(compilationContext);
            executionContext.Entity = record;

            var value = func(executionContext);
            Assert.AreEqual(compilationContext.PrimaryDataSource.DefaultCollation.ToSqlString("2022-01-02"), value);
        }

        [TestMethod]
        public void LikeWithEmbeddedReturns()
        {
            var expr = new LikePredicate
            {
                FirstExpression = Col("content"),
                SecondExpression = new StringLiteral { Value = "%func%" }
            };
            var schema = new NodeSchema(new Dictionary<string, IColumnDefinition>
            {
                ["content"] = new ExecutionPlan.ColumnDefinition(DataTypeHelpers.VarChar(100, Collation.USEnglish, CollationLabel.Implicit), true, false)
            }, new Dictionary<string, IReadOnlyList<string>>(), null, Array.Empty<string>());
            var parameterTypes = new Dictionary<string, DataTypeReference>();
            var options = new StubOptions();
            var compilationContext = new ExpressionCompilationContext(_localDataSources, options, parameterTypes, schema, null);
            var func = expr.Compile(compilationContext);

            var record = new Entity
            {
                ["content"] = Collation.USEnglish.ToSqlString(@"function func() {
   console.log(""hello"");
};")
            };
            var executionContext = new ExpressionExecutionContext(compilationContext);
            executionContext.Entity = record;

            var value = func(executionContext);
            Assert.AreEqual(true, value);
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
