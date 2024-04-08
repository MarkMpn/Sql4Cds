using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using FakeXrmEasy;
using MarkMpn.Sql4Cds.Engine.ExecutionPlan;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Xrm.Sdk;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class ExecutionPlanNodeTests : FakeXrmEasyTestsBase
    {
        [TestMethod]
        public void ConstantScanTest()
        {
            var node = new ConstantScanNode
            {
                Values =
                {
                    new Dictionary<string, ScalarExpression>
                    {
                        ["firstname"] = new StringLiteral { Value = "Mark" }
                    }
                },
                Schema =
                {
                    ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                },
                Alias = "test"
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();
            
            Assert.AreEqual(1, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["test.firstname"]).Value);
        }

        [TestMethod]
        public void FilterNodeTest()
        {
            var node = new FilterNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "test"
                },
                Filter = new BooleanComparisonExpression
                {
                    FirstExpression = new ColumnReferenceExpression
                    {
                        MultiPartIdentifier = new MultiPartIdentifier
                        {
                            Identifiers =
                            {
                                new Identifier { Value = "firstname" }
                            }
                        }
                    },
                    ComparisonType = BooleanComparisonType.Equals,
                    SecondExpression = new StringLiteral { Value = "Mark" }
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();

            Assert.AreEqual(1, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["test.firstname"]).Value);
        }

        [TestMethod]
        public void MergeJoinInnerTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key1" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Webber" }
                        }
                    },
                    Schema =
                    {
                        ["key2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["lastname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key2" } }
                    }
                },
                JoinType = QualifiedJoinType.Inner
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();

            Assert.AreEqual(2, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
        }

        [TestMethod]
        public void MergeJoinLeftOuterTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key1" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Hamill" }
                        }
                    },
                    Schema =
                    {
                        ["key2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["lastname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key2" } }
                    }
                },
                JoinType = QualifiedJoinType.LeftOuter
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
            Assert.AreEqual("Joe", ((SqlString)results[2]["f.firstname"]).Value);
            Assert.IsTrue(((SqlString)results[2]["l.lastname"]).IsNull);
        }

        [TestMethod]
        public void MergeJoinRightOuterTest()
        {
            var node = new MergeJoinNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key1" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Hamill" }
                        }
                    },
                    Schema =
                    {
                        ["key2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["lastname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key2" } }
                    }
                },
                JoinType = QualifiedJoinType.RightOuter
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
            Assert.IsTrue(((SqlString)results[2]["f.firstname"]).IsNull);
            Assert.AreEqual("Hamill", ((SqlString)results[2]["l.lastname"]).Value);
        }

        [TestMethod]
        public void AssertionTest()
        {
            var node = new AssertNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["name"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["name"] = new StringLiteral { Value = "Carrington" }
                        }
                    },
                    Schema =
                    {
                        ["name"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "test"
                },
                Assertion = e => e.GetAttributeValue<SqlString>("test.name").Value == "Mark",
                ErrorMessage = "Only Mark is allowed"
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).GetEnumerator();

            Assert.IsTrue(results.MoveNext());
            Assert.AreEqual("Mark", results.Current.GetAttributeValue<SqlString>("test.name").Value);

            var ex = Assert.ThrowsException<QueryExecutionException>(() => results.MoveNext());
            Assert.AreEqual(node.ErrorMessage, ex.Message);
        }

        [TestMethod]
        public void ComputeScalarTest()
        {
            var node = new ComputeScalarNode
            {
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new IntegerLiteral { Value = "1" },
                            ["value2"] = new IntegerLiteral { Value = "2" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new IntegerLiteral { Value = "3" },
                            ["value2"] = new IntegerLiteral { Value = "4" }
                        }
                    },
                    Schema =
                    {
                        ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["value2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                    }
                },
                Columns =
                {
                    ["mul"] = new BinaryExpression
                    {
                        FirstExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = "value1" } }
                            }
                        },
                        BinaryExpressionType = BinaryExpressionType.Multiply,
                        SecondExpression = new ColumnReferenceExpression
                        {
                            MultiPartIdentifier = new MultiPartIdentifier
                            {
                                Identifiers = { new Identifier { Value = "value2" } }
                            }
                        }
                    }
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlInt32>("mul").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 2, 12 }, results);
        }

        [TestMethod]
        public void DistinctTest()
        {
            var node = new DistinctNode
            {
                Columns = { "test.value1" },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new IntegerLiteral { Value = "1" },
                            ["value2"] = new IntegerLiteral { Value = "1" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new IntegerLiteral { Value = "3" },
                            ["value2"] = new IntegerLiteral { Value = "2" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new IntegerLiteral { Value = "1" },
                            ["value2"] = new IntegerLiteral { Value = "3" }
                        }
                    },
                    Schema =
                    {
                        ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["value2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlInt32>("test.value1").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 3 }, results);
        }

        [TestMethod]
        public void DistinctCaseInsensitiveTest()
        {
            var node = new DistinctNode
            {
                Columns = { "test.value1" },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "hello" },
                            ["value2"] = new IntegerLiteral { Value = "1" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "world" },
                            ["value2"] = new IntegerLiteral { Value = "2" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "Hello" },
                            ["value2"] = new IntegerLiteral { Value = "3" }
                        }
                    },
                    Schema =
                    {
                        ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false),
                        ["value2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlString>("test.value1").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { "hello", "world" }, results);
        }

        [TestMethod]
        public void SortNodeTest()
        {
            var node = new SortNode
            {
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = { new Identifier { Value = "test" }, new Identifier { Value = "value1" } } } },
                        SortOrder = SortOrder.Ascending
                    },
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = { new Identifier{Value = "test" }, new Identifier { Value = "value2" } } } },
                        SortOrder = SortOrder.Descending
                    }
                },
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "hello" },
                            ["value2"] = new IntegerLiteral { Value = "1" },
                            ["expectedorder"] = new IntegerLiteral { Value = "2" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "world" },
                            ["value2"] = new IntegerLiteral { Value = "2" },
                            ["expectedorder"] = new IntegerLiteral { Value = "3" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "Hello" },
                            ["value2"] = new IntegerLiteral { Value = "3" },
                            ["expectedorder"] = new IntegerLiteral { Value = "1" }
                        }
                    },
                    Schema =
                    {
                        ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false),
                        ["value2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["expectedorder"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlInt32>("test.expectedorder").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2, 3 }, results);
        }

        [TestMethod]
        public void SortNodePresortedTest()
        {
            var node = new SortNode
            {
                Sorts =
                {
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = {new Identifier{Value = "test" }, new Identifier { Value = "value1" } } } },
                        SortOrder = SortOrder.Ascending
                    },
                    new ExpressionWithSortOrder
                    {
                        Expression = new ColumnReferenceExpression{MultiPartIdentifier = new MultiPartIdentifier{Identifiers = { new Identifier { Value = "test" }, new Identifier { Value = "value2" } } } },
                        SortOrder = SortOrder.Descending
                    }
                },
                PresortedCount = 1,
                Source = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "hello" },
                            ["value2"] = new IntegerLiteral { Value = "1" },
                            ["expectedorder"] = new IntegerLiteral { Value = "1" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "world" },
                            ["value2"] = new IntegerLiteral { Value = "2" },
                            ["expectedorder"] = new IntegerLiteral { Value = "2" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "Hello" },
                            ["value2"] = new IntegerLiteral { Value = "3" },
                            ["expectedorder"] = new IntegerLiteral { Value = "4" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["value1"] = new StringLiteral { Value = "Hello" },
                            ["value2"] = new IntegerLiteral { Value = "4" },
                            ["expectedorder"] = new IntegerLiteral { Value = "3" }
                        }
                    },
                    Schema =
                    {
                        ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false),
                        ["value2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["expectedorder"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlInt32>("test.expectedorder").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2, 3, 4 }, results);
        }

        [TestMethod]
        public void TableSpoolTest()
        {
            var source = new ConstantScanNode
            {
                Values =
                {
                    new Dictionary<string, ScalarExpression>
                    {
                        ["value1"] = new IntegerLiteral { Value = "1" }
                    },
                    new Dictionary<string, ScalarExpression>
                    {
                        ["value1"] = new IntegerLiteral { Value = "2" }
                    }
                },
                Schema =
                {
                    ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                },
                Alias = "test"
            };

            var spool = new TableSpoolNode { Source = source };

            var results1 = spool.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlInt32>("test.value1").Value)
                .ToArray();

            var results2 = spool.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => e.GetAttributeValue<SqlInt32>("test.value1").Value)
                .ToArray();

            CollectionAssert.AreEqual(new[] { 1, 2 }, results1);
            CollectionAssert.AreEqual(new[] { 1, 2 }, results2);
            Assert.AreEqual(1, source.ExecutionCount);
        }

        [TestMethod]
        public void CaseInsenstiveHashMatchAggregateNodeTest()
        {
            var source = new ConstantScanNode
            {
                Values =
                {
                    new Dictionary<string, ScalarExpression>
                    {
                        ["value1"] = new StringLiteral { Value = "hello" }
                    },
                    new Dictionary<string, ScalarExpression>
                    {
                        ["value1"] = new StringLiteral { Value = "Hello" }
                    }
                },
                Schema =
                {
                    ["value1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                },
                Alias = "src"
            };

            var spool = new HashMatchAggregateNode
            {
                Source = source,
                GroupBy =
                {
                    "src.value1".ToColumnReference()
                },
                Aggregates =
                {
                    ["count"] = new Aggregate { AggregateType = AggregateType.CountStar }
                }
            };

            var results = spool.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null))
                .Select(e => new { Name = e.GetAttributeValue<SqlString>("src.value1").Value, Count = e.GetAttributeValue<SqlInt32>("count").Value })
                .ToArray();

            CollectionAssert.AreEqual(new[] { new { Name = "hello", Count = 2 } }, results);
        }

        [TestMethod]
        public void SqlTransformSchemaOnly()
        {
            var sql = "SELECT name FROM account; DECLARE @id uniqueidentifier; SELECT name FROM account WHERE accountid = @id";
            var transformed = SqlNode.ApplyCommandBehavior(sql, System.Data.CommandBehavior.SchemaOnly, new StubOptions());
            transformed = Regex.Replace(transformed, "[ \\r\\n]+", " ").Trim();

            Assert.AreEqual("SELECT name FROM account WHERE 0 = 1; DECLARE @id AS UNIQUEIDENTIFIER; SELECT name FROM account WHERE accountid = @id AND 0 = 1;", transformed);
        }

        [TestMethod]
        public void SqlTransformSingleRow()
        {
            var sql = "SELECT name FROM account; DECLARE @id uniqueidentifier; SELECT name FROM account WHERE accountid = @id";
            var transformed = SqlNode.ApplyCommandBehavior(sql, System.Data.CommandBehavior.SingleRow, new StubOptions());
            transformed = Regex.Replace(transformed, "[ \\r\\n]+", " ").Trim();

            Assert.AreEqual("SELECT TOP 1 name FROM account; DECLARE @id AS UNIQUEIDENTIFIER;", transformed);
        }

        [TestMethod]
        public void SqlTransformSingleResult()
        {
            var sql = "SELECT name FROM account; DECLARE @id uniqueidentifier; SELECT name FROM account WHERE accountid = @id";
            var transformed = SqlNode.ApplyCommandBehavior(sql, System.Data.CommandBehavior.SingleResult, new StubOptions());
            transformed = Regex.Replace(transformed, "[ \\r\\n]+", " ").Trim();

            Assert.AreEqual("SELECT name FROM account; DECLARE @id AS UNIQUEIDENTIFIER;", transformed);
        }

        [TestMethod]
        public void AggregateInitialTest()
        {
            var aggregate = CreateAggregateTest();
            var result = aggregate.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).Single();

            Assert.AreEqual(SqlInt32.Null, result["min"]);
            Assert.AreEqual(SqlInt32.Null, result["max"]);
            Assert.AreEqual((SqlInt32)0, result["sum"]);
            Assert.AreEqual((SqlInt32)0, result["sum_distinct"]);
            Assert.AreEqual((SqlInt32)0, result["count"]);
            Assert.AreEqual((SqlInt32)0, result["count_distinct"]);
            Assert.AreEqual((SqlInt32)0, result["countstar"]);
            Assert.AreEqual(SqlInt32.Null, result["avg"]);
            Assert.AreEqual(SqlInt32.Null, result["avg_distinct"]);
            Assert.AreEqual(SqlInt32.Null, result["first"]);
        }

        [TestMethod]
        public void AggregateSingleValueTest()
        {
            var aggregate = CreateAggregateTest(1);
            var result = aggregate.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).Single();

            Assert.AreEqual((SqlInt32)1, result["min"]);
            Assert.AreEqual((SqlInt32)1, result["max"]);
            Assert.AreEqual((SqlInt32)1, result["sum"]);
            Assert.AreEqual((SqlInt32)1, result["sum_distinct"]);
            Assert.AreEqual((SqlInt32)1, result["count"]);
            Assert.AreEqual((SqlInt32)1, result["count_distinct"]);
            Assert.AreEqual((SqlInt32)1, result["countstar"]);
            Assert.AreEqual((SqlInt32)1, result["avg"]);
            Assert.AreEqual((SqlInt32)1, result["avg_distinct"]);
            Assert.AreEqual((SqlInt32)1, result["first"]);
        }

        [TestMethod]
        public void AggregateTwoEqualValuesTest()
        {
            var aggregate = CreateAggregateTest(1, 1);
            var result = aggregate.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).Single();

            Assert.AreEqual((SqlInt32)1, result["min"]);
            Assert.AreEqual((SqlInt32)1, result["max"]);
            Assert.AreEqual((SqlInt32)2, result["sum"]);
            Assert.AreEqual((SqlInt32)1, result["sum_distinct"]);
            Assert.AreEqual((SqlInt32)2, result["count"]);
            Assert.AreEqual((SqlInt32)1, result["count_distinct"]);
            Assert.AreEqual((SqlInt32)2, result["countstar"]);
            Assert.AreEqual((SqlInt32)1, result["avg"]);
            Assert.AreEqual((SqlInt32)1, result["avg_distinct"]);
            Assert.AreEqual((SqlInt32)1, result["first"]);
        }

        [TestMethod]
        public void AggregateMultipleValuesTest()
        {
            var aggregate = CreateAggregateTest(1, 3, 1, 1);
            var result = aggregate.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).Single();

            Assert.AreEqual((SqlInt32)1, result["min"]);
            Assert.AreEqual((SqlInt32)3, result["max"]);
            Assert.AreEqual((SqlInt32)6, result["sum"]);
            Assert.AreEqual((SqlInt32)4, result["sum_distinct"]);
            Assert.AreEqual((SqlInt32)4, result["count"]);
            Assert.AreEqual((SqlInt32)2, result["count_distinct"]);
            Assert.AreEqual((SqlInt32)4, result["countstar"]);
            Assert.AreEqual((SqlInt32)1, result["avg"]);
            Assert.AreEqual((SqlInt32)2, result["avg_distinct"]);
            Assert.AreEqual((SqlInt32)1, result["first"]);
        }

        private HashMatchAggregateNode CreateAggregateTest(params int[] values)
        {
            var source = new ConstantScanNode
            {
                Schema =
                {
                    ["i"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false)
                },
                Alias = "l"
            };

            foreach (var value in values)
            {
                source.Values.Add(new Dictionary<string, ScalarExpression>
                {
                    ["i"] = new IntegerLiteral { Value = value.ToString() }
                });
            }

            var aggregate = new HashMatchAggregateNode
            {
                Aggregates =
                {
                    ["min"] = new Aggregate
                    {
                        AggregateType = AggregateType.Min,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["max"] = new Aggregate
                    {
                        AggregateType = AggregateType.Max,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["sum"] = new Aggregate
                    {
                        AggregateType = AggregateType.Sum,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["sum_distinct"] = new Aggregate
                    {
                        AggregateType = AggregateType.Sum,
                        Distinct = true,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["count"] = new Aggregate
                    {
                        AggregateType = AggregateType.Count,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["count_distinct"] = new Aggregate
                    {
                        AggregateType = AggregateType.Count,
                        Distinct = true,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["countstar"] = new Aggregate
                    {
                        AggregateType = AggregateType.CountStar
                    },
                    ["avg"] = new Aggregate
                    {
                        AggregateType = AggregateType.Average,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["avg_distinct"] = new Aggregate
                    {
                        AggregateType = AggregateType.Average,
                        Distinct = true,
                        SqlExpression = "l.i".ToColumnReference()
                    },
                    ["first"] = new Aggregate
                    {
                        AggregateType = AggregateType.First,
                        SqlExpression = "l.i".ToColumnReference()
                    }
                },
                Source = source
            };

            return aggregate;
        }

        [TestMethod]
        public void NestedLoopJoinInnerTest()
        {
            var node = new NestedLoopNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "f"
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Webber" }
                        }
                    },
                    Schema =
                    {
                        ["key2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["lastname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "l"
                },
                JoinType = QualifiedJoinType.Inner,
                JoinCondition = new BooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpression = "f.key1".ToColumnReference(),
                    SecondExpression = "l.key2".ToColumnReference()
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();

            Assert.AreEqual(2, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
        }

        [TestMethod]
        public void NestedLoopJoinLeftOuterTest()
        {
            var node = new NestedLoopNode
            {
                LeftSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key1"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key1"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["firstname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "f"
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key2"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Hamill" }
                        }
                    },
                    Schema =
                    {
                        ["key2"] = new ExecutionPlan.ColumnDefinition(typeof(SqlInt32).ToSqlType(null), true, false),
                        ["lastname"] = new ExecutionPlan.ColumnDefinition(typeof(SqlString).ToSqlType(null), true, false)
                    },
                    Alias = "l"
                },
                JoinType = QualifiedJoinType.LeftOuter,
                JoinCondition = new BooleanComparisonExpression
                {
                    ComparisonType = BooleanComparisonType.Equals,
                    FirstExpression = "f.key1".ToColumnReference(),
                    SecondExpression = "l.key2".ToColumnReference()
                }
            };

            var results = node.Execute(new NodeExecutionContext(_localDataSources, new StubOptions(), null, null, null)).ToArray();

            Assert.AreEqual(3, results.Length);
            Assert.AreEqual("Mark", ((SqlString)results[0]["f.firstname"]).Value);
            Assert.AreEqual("Carrington", ((SqlString)results[0]["l.lastname"]).Value);
            Assert.AreEqual("Mark", ((SqlString)results[1]["f.firstname"]).Value);
            Assert.AreEqual("Twain", ((SqlString)results[1]["l.lastname"]).Value);
            Assert.AreEqual("Joe", ((SqlString)results[2]["f.firstname"]).Value);
            Assert.IsTrue(((SqlString)results[2]["l.lastname"]).IsNull);
        }
    }
}
