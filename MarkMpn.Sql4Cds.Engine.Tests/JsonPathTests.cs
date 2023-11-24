using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace MarkMpn.Sql4Cds.Engine.Tests
{
    [TestClass]
    public class JsonPathTests
    {
        [TestMethod]
        public void DollarOnly()
        {
            var path = new JsonPath("$");

            var json = JsonDocument.Parse(@"
{
  ""path"": {
    ""to"": {
      ""sub-object"": ""hello world""
    }
  }
}");

            var value = path.Evaluate(json.RootElement);
            Assert.AreEqual(value, json.RootElement);
        }

        [TestMethod]
        public void QuotedPropertyName()
        {
            var path = new JsonPath("$.path.to.\"sub-object\"");

            var json = JsonDocument.Parse(@"
{
  ""path"": {
    ""to"": {
      ""sub-object"": ""hello world""
    }
  }
}");

            var value = path.Evaluate(json.RootElement);
            Assert.AreEqual("hello world", value.Value.GetString());
        }

        [TestMethod]
        public void DuplicatePathsReturnsFirstMatch()
        {
            var path = new JsonPath("$.person.name");

            var json = JsonDocument.Parse(@"
{
  ""person"": {
    ""name"": ""Mark"",
    ""name"": ""Carrington""
  }
}");

            var value = path.Evaluate(json.RootElement);
            Assert.AreEqual("Mark", value.Value.GetString());
        }

        [TestMethod]
        public void DefaultModeLax()
        {
            var path = new JsonPath("$.path.to.\"sub-object\"");
            Assert.AreEqual(JsonPathMode.Lax, path.Mode);
        }

        [TestMethod]
        public void ExplicitModeLax()
        {
            var path = new JsonPath("lax $.path.to.\"sub-object\"");
            Assert.AreEqual(JsonPathMode.Lax, path.Mode);
        }

        [TestMethod]
        public void ExplicitModeStrict()
        {
            var path = new JsonPath("strict $.path.to.\"sub-object\"");
            Assert.AreEqual(JsonPathMode.Strict, path.Mode);
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnUnknownMode()
        {
            new JsonPath("laxtrict $.path.to.\"sub-object\"");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnExtraQuotes()
        {
            new JsonPath("laxtrict $.path.to.\"\"sub-object\"\"");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnMissingLeadingDollar()
        {
            new JsonPath("path.to.\"sub-object\"");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnDoublePeriod()
        {
            new JsonPath("$.path..to.\"sub-object\"");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnTrailingPeriod()
        {
            new JsonPath("$.path.to.\"sub-object\".");
        }

        [TestMethod]
        public void ArrayIndexer()
        {
            var path = new JsonPath("$.path.to.\"sub-object\"[1]");

            var json = JsonDocument.Parse(@"
{
  ""path"": {
    ""to"": {
      ""sub-object"": [ ""hello"", ""world"" ]
    }
  }
}");

            var value = path.Evaluate(json.RootElement);
            Assert.AreEqual("world", value.Value.GetString());
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnNegativeIndexer()
        {
            new JsonPath("$.path.to.\"sub-object\"[-1]");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnAlphaIndexer()
        {
            new JsonPath("$.path.to.\"sub-object\"[x]");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnMissingCloseBracket()
        {
            new JsonPath("$.path.to.\"sub-object\"[1");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnMissingOpeningBracket()
        {
            new JsonPath("$.path.to1]");
        }

        [TestMethod]
        public void NestedArrayIndexer()
        {
            var path = new JsonPath("$.path.to.\"sub-object\"[1][0]");

            var json = JsonDocument.Parse(@"
{
  ""path"": {
    ""to"": {
      ""sub-object"": [ [ ""hel"", ""lo"" ], [ ""wor"", ""ld"" ] ]
    }
  }
}");

            var value = path.Evaluate(json.RootElement);
            Assert.AreEqual("wor", value.Value.GetString());
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnDollarNotAtStart()
        {
            new JsonPath("$.path.$.to");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnUnquotedDollar()
        {
            new JsonPath("$.path$");
        }

        [TestMethod]
        [ExpectedException(typeof(JsonException))]
        public void FailsOnLeadingNumber()
        {
            new JsonPath("$.1path");
        }

        [TestMethod]
        public void TrailingNumber()
        {
            var path = new JsonPath("$.path1.to2.\"sub-object\"");

            var json = JsonDocument.Parse(@"
{
  ""path1"": {
    ""to2"": {
      ""sub-object"": ""hello world""
    }
  }
}");

            var value = path.Evaluate(json.RootElement);
            Assert.AreEqual("hello world", value.Value.GetString());
        }
    }
}
