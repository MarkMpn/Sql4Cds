import assert from "node:assert/strict";
import test from "node:test";
import { detectStructuredValue } from "../src/structuredValue";

test("detects and prettifies JSON objects and arrays but ignores scalars", () => {
  assert.deepEqual(detectStructuredValue('{"a":[1,true]}'), { language: "json", formatted: '{\n  "a": [\n    1,\n    true\n  ]\n}' });
  assert.equal(detectStructuredValue('"a string"'), undefined);
  assert.equal(detectStructuredValue("123"), undefined);
});

test("detects and prettifies XML and FetchXML", () => {
  assert.deepEqual(detectStructuredValue('<fetch top="1"><entity name="account" /></fetch>'), {
    language: "xml",
    formatted: '<fetch top="1">\n  <entity name="account" />\n</fetch>'
  });
  assert.equal(detectStructuredValue("<a><b></a>"), undefined);
  assert.equal(detectStructuredValue("normal < text"), undefined);
});

test("XML tokenizer handles quoted angle brackets, comments, declarations, and CDATA", () => {
  const detected = detectStructuredValue('<?xml version="1.0"?><root value=">"><!-- x --><![CDATA[a < b]]></root>');
  assert.equal(detected?.language, "xml");
  assert.match(detected?.formatted ?? "", /<root value=">">/);
  assert.match(detected?.formatted ?? "", /<!\[CDATA\[a < b\]\]>/);
});

test("rejects unterminated XML comments and CDATA without hanging", () => {
  assert.equal(detectStructuredValue("<root><!-- unfinished</root>"), undefined);
  assert.equal(detectStructuredValue("<root><![CDATA[unfinished</root>"), undefined);
});
