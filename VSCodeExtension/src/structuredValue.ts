export type StructuredLanguage = "json" | "xml";

export interface StructuredValue {
  language: StructuredLanguage;
  formatted: string;
}

/** Detects only JSON containers and well-formed XML, avoiding ordinary scalar values. */
export function detectStructuredValue(value: string): StructuredValue | undefined {
  const trimmed = value.trim();
  if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
    try {
      const parsed: unknown = JSON.parse(trimmed);
      if (parsed !== null && typeof parsed === "object") {
        return { language: "json", formatted: JSON.stringify(parsed, undefined, 2) };
      }
    } catch { /* Try XML below. */ }
  }
  const tokens = tokenizeXml(trimmed);
  if (tokens && validateXml(tokens)) {
    return { language: "xml", formatted: formatXml(tokens) };
  }
  return undefined;
}

interface XmlToken { kind: "tag" | "text"; value: string; }

function tokenizeXml(xml: string): XmlToken[] | undefined {
  if (!xml.startsWith("<")) { return undefined; }
  const tokens: XmlToken[] = [];
  let start = 0;
  while (start < xml.length) {
    if (xml[start] !== "<") {
      const end = xml.indexOf("<", start);
      const next = end < 0 ? xml.length : end;
      tokens.push({ kind: "text", value: xml.slice(start, next) });
      start = next;
      continue;
    }
    const end = findTagEnd(xml, start);
    if (end < 0) { return undefined; }
    tokens.push({ kind: "tag", value: xml.slice(start, end + 1) });
    start = end + 1;
  }
  return tokens;
}

function findTagEnd(xml: string, start: number): number {
  if (xml.startsWith("<!--", start)) {
    const end = xml.indexOf("-->", start + 4);
    return end < 0 ? -1 : end + 2;
  }
  if (xml.startsWith("<![CDATA[", start)) {
    const end = xml.indexOf("]]>", start + 9);
    return end < 0 ? -1 : end + 2;
  }
  let quote = "";
  for (let index = start + 1; index < xml.length; index++) {
    const character = xml[index];
    if (quote) { if (character === quote) { quote = ""; } }
    else if (character === '"' || character === "'") { quote = character; }
    else if (character === ">") { return index; }
  }
  return -1;
}

function validateXml(tokens: readonly XmlToken[]): boolean {
  const stack: string[] = [];
  let roots = 0;
  for (const token of tokens) {
    if (token.kind === "text") {
      if (!stack.length && token.value.trim()) { return false; }
      continue;
    }
    const tag = token.value;
    if (tag.startsWith("<?") || tag.startsWith("<!--") || tag.startsWith("<![CDATA[") || /^<!DOCTYPE\b/i.test(tag)) { continue; }
    const close = tag.match(/^<\/\s*([^\s>]+)\s*>$/);
    if (close) { if (stack.pop() !== close[1]) { return false; } continue; }
    // findTagEnd has already accounted for quoted `>` characters in attributes.
    const open = tag.match(/^<\s*([^\s/>]+)/);
    if (!open) { return false; }
    if (!stack.length) { roots++; }
    if (!/\/\s*>$/.test(tag)) { stack.push(open[1]); }
  }
  return roots === 1 && stack.length === 0;
}

function formatXml(tokens: readonly XmlToken[]): string {
  const lines: string[] = [];
  let depth = 0;
  for (const token of tokens) {
    const value = token.value.trim();
    if (!value) { continue; }
    if (token.kind === "text") {
      if (lines.length && !lines[lines.length - 1].endsWith(">")) { lines[lines.length - 1] += value; }
      else { lines.push(`${"  ".repeat(depth)}${value}`); }
      continue;
    }
    const closing = /^<\//.test(value);
    const standalone = /^<\?|^<!|\/\s*>$/.test(value);
    if (closing) { depth = Math.max(0, depth - 1); }
    if (closing && lines.length && !lines[lines.length - 1].endsWith(">")) { lines[lines.length - 1] += value; }
    else { lines.push(`${"  ".repeat(depth)}${value}`); }
    if (!closing && !standalone) { depth++; }
  }
  return lines.join("\n");
}
