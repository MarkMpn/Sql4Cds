import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const extensionRoot = dirname(dirname(fileURLToPath(import.meta.url)));
const serviceDll = join(extensionRoot, "out", "sql4cdstoolsservice", "MarkMpn.Sql4Cds.LanguageServer.dll");
const logDir = await mkdtemp(join(tmpdir(), "sql4cds-smoke-"));
const pending = new Map();
const notifications = new Map();
let buffer = Buffer.alloc(0);
let stderr = "";

const service = spawn("dotnet", [serviceDll, `--log-dir=${logDir}`], {
  stdio: ["pipe", "pipe", "pipe"],
  env: { ...process.env, SQL4CDS_DATA_DIR: logDir, SQL4CDS_DISABLE_TELEMETRY: "1" }
});

service.stderr.setEncoding("utf8");
service.stderr.on("data", chunk => { stderr += chunk; });
service.stdout.on("data", chunk => {
  buffer = Buffer.concat([buffer, chunk]);
  readMessages();
});
service.on("error", error => rejectAll(error));
service.on("exit", (code, signal) => {
  if (pending.size > 0 || notifications.size > 0) {
    rejectAll(new Error(`Language service exited early (${signal ?? code}). ${stderr.trim()}`));
  }
});

try {
  const initialized = await request(1, "initialize", {
    processId: process.pid,
    rootUri: null,
    capabilities: {},
    clientInfo: { name: "SQL 4 CDS release smoke test" }
  });
  if (!initialized?.capabilities) { throw new Error("Initialize response did not contain server capabilities."); }

  notify("initialized", {});
  const diagnostics = waitForNotification("textDocument/publishDiagnostics");
  notify("textDocument/didOpen", { textDocument: { uri: "untitled:sql4cds-smoke", languageId: "sql4cds", version: 1, text: "SELECT 1" } });
  const published = await diagnostics;
  if (Array.isArray(published) || published?.uri !== "untitled:sql4cds-smoke" || !Array.isArray(published.diagnostics)) {
    throw new Error("Editor diagnostics must use an LSP named-parameter object.");
  }
  notify("textDocument/didClose", { textDocument: { uri: "untitled:sql4cds-smoke" } });
  const exited = new Promise(resolve => service.once("exit", (code, signal) => resolve({ code, signal })));
  service.stdin.end();
  let timer;
  let exit;
  try {
    exit = await Promise.race([
      exited,
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error("Language service did not stop after closing its input.")), 10_000); })
    ]);
  } finally { clearTimeout(timer); }
  if (exit.code !== 0) { throw new Error(`Language service exited with code ${exit.code}. ${stderr.trim()}`); }
  process.stdout.write("SQL 4 CDS language service passed initialization, editor diagnostics, and shutdown smoke tests.\n");
} finally {
  if (service.exitCode === null) { service.kill(); }
  await rm(logDir, { recursive: true, force: true });
}

function request(id, method, params) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      pending.delete(id);
      reject(new Error(`Timed out waiting for ${method}. ${stderr.trim()}`));
    }, 20_000);
    pending.set(id, { resolve, reject, timer });
    send({ jsonrpc: "2.0", id, method, params });
  });
}

function waitForNotification(method) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { notifications.delete(method); reject(new Error(`Timed out waiting for ${method}.`)); }, 20_000);
    notifications.set(method, { resolve, reject, timer });
  });
}

function notify(method, params) {
  const message = { jsonrpc: "2.0", method };
  if (params !== undefined) { message.params = params; }
  send(message);
}

function send(message) {
  const json = JSON.stringify(message);
  service.stdin.write(`Content-Length: ${Buffer.byteLength(json)}\r\n\r\n${json}`);
}

function readMessages() {
  while (true) {
    const headerEnd = buffer.indexOf("\r\n\r\n");
    if (headerEnd === -1) { return; }
    const header = buffer.subarray(0, headerEnd).toString("ascii");
    const lengthMatch = /(?:^|\r\n)Content-Length:\s*(\d+)/i.exec(header);
    if (!lengthMatch) { throw new Error(`Invalid language service header: ${header}`); }
    const length = Number(lengthMatch[1]);
    const messageEnd = headerEnd + 4 + length;
    if (buffer.length < messageEnd) { return; }
    const message = JSON.parse(buffer.subarray(headerEnd + 4, messageEnd).toString("utf8"));
    buffer = buffer.subarray(messageEnd);
    if (message.id === undefined) {
      const notification = notifications.get(message.method);
      if (notification) {
        clearTimeout(notification.timer);
        notifications.delete(message.method);
        notification.resolve(message.params);
      }
      continue;
    }
    const completion = pending.get(message.id);
    if (!completion) { continue; }
    clearTimeout(completion.timer);
    pending.delete(message.id);
    if (message.error) { completion.reject(new Error(message.error.message ?? JSON.stringify(message.error))); }
    else { completion.resolve(message.result); }
  }
}

function rejectAll(error) {
  for (const completion of [...pending.values(), ...notifications.values()]) {
    clearTimeout(completion.timer);
    completion.reject(error);
  }
  pending.clear();
  notifications.clear();
}
