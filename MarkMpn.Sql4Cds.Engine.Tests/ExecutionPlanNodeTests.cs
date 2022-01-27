using System;
using System.Collections.Generic;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
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
                    ["firstname"] = typeof(SqlString).ToSqlType()
                },
                Alias = "test"
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

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
                        ["firstname"] = typeof(SqlString).ToSqlType()
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

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

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
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32).ToSqlType(),
                        ["firstname"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Webber" }
                        }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32).ToSqlType(),
                        ["lastname"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.Inner
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

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
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32).ToSqlType(),
                        ["firstname"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Hamill" }
                        }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32).ToSqlType(),
                        ["lastname"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.LeftOuter
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

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
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["firstname"] = new StringLiteral { Value = "Mark" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "2" },
                            ["firstname"] = new StringLiteral { Value = "Joe" }
                        }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32).ToSqlType(),
                        ["firstname"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "f"
                },
                LeftAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "f" }, new Identifier { Value = "key" } }
                    }
                },
                RightSource = new ConstantScanNode
                {
                    Values =
                    {
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Carrington" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "1" },
                            ["lastname"] = new StringLiteral { Value = "Twain" }
                        },
                        new Dictionary<string, ScalarExpression>
                        {
                            ["key"] = new IntegerLiteral { Value = "3" },
                            ["lastname"] = new StringLiteral { Value = "Hamill" }
                        }
                    },
                    Schema =
                    {
                        ["key"] = typeof(SqlInt32).ToSqlType(),
                        ["lastname"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "l"
                },
                RightAttribute = new ColumnReferenceExpression
                {
                    MultiPartIdentifier = new MultiPartIdentifier
                    {
                        Identifiers = { new Identifier { Value = "l" }, new Identifier { Value = "key" } }
                    }
                },
                JoinType = QualifiedJoinType.RightOuter
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).ToArray();

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
                        ["name"] = typeof(SqlString).ToSqlType()
                    },
                    Alias = "test"
                },
                Assertion = e => e.GetAttributeValue<SqlString>("test.name").Value == "Mark",
                ErrorMessage = "Only Mark is allowed"
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null).GetEnumerator();

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
                        ["value1"] = typeof(SqlInt32).ToSqlType(),
                        ["value2"] = typeof(SqlInt32).ToSqlType()
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

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
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
                        ["value1"] = typeof(SqlInt32).ToSqlType(),
                        ["value2"] = typeof(SqlInt32).ToSqlType()
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
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
                        ["value1"] = typeof(SqlString).ToSqlType(),
                        ["value2"] = typeof(SqlInt32).ToSqlType()
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
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
                        ["value1"] = typeof(SqlString).ToSqlType(),
                        ["value2"] = typeof(SqlInt32).ToSqlType(),
                        ["expectedorder"] = typeof(SqlInt32).ToSqlType()
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
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
                        ["value1"] = typeof(SqlString).ToSqlType(),
                        ["value2"] = typeof(SqlInt32).ToSqlType(),
                        ["expectedorder"] = typeof(SqlInt32).ToSqlType()
                    },
                    Alias = "test"
                }
            };

            var results = node.Execute(_dataSources, new StubOptions(), null, null)
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
                    ["value1"] = typeof(SqlInt32).ToSqlType()
                },
                Alias = "test"
            };

            var spool = new TableSpoolNode { Source = source };

            var results1 = spool.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => e.GetAttributeValue<SqlInt32>("test.value1").Value)
                .ToArray();

            var results2 = spool.Execute(_dataSources, new StubOptions(), null, null)
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
                    ["value1"] = typeof(SqlString).ToSqlType()
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

            var results = spool.Execute(_dataSources, new StubOptions(), null, null)
                .Select(e => new { Name = e.GetAttributeValue<SqlString>("src.value1").Value, Count = e.GetAttributeValue<SqlInt32>("count").Value })
                .ToArray();

            CollectionAssert.AreEqual(new[] { new { Name = "hello", Count = 2 } }, results);
        }
    }
}
