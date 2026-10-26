const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { test } = require("node:test");
const ts = require("typescript");

function load(relativePath, dependencies = {}, globals = {}) {
    const filename = path.resolve(__dirname, "..", "src", relativePath);
    const module = { exports: {} };
    const code = ts.transpileModule(fs.readFileSync(filename, "utf8"), {
        compilerOptions: { module: ts.ModuleKind.CommonJS, target: ts.ScriptTarget.ES2022 },
    }).outputText;
    vm.runInNewContext(code, {
        module,
        exports: module.exports,
        require: (name) => dependencies[name] ?? {},
        ...globals,
    }, { filename });
    return module.exports;
}

const layout = load("executionPlan/layout.ts");
const { getNodeImageName } = load("executionPlan/nodeImageName.ts");
const { getFetchXmlQuery } = load("executionPlan/executionPlanView.ts");
const imagesDir = path.resolve(__dirname, "..", "..", "MarkMpn.Sql4Cds.Controls", "Images");
const images = new Set(fs.readdirSync(imagesDir).filter((f) => f.endsWith(".ico")).map((f) => f.slice(0, -4)));

// Each line is 10px wide per character and 16px high
const measure = (text) => {
    const lines = text.split("\n");
    return { width: Math.max(...lines.map((l) => l.length * 10)), height: lines.length * 16 };
};

function node(name, children = [], rowCounts = [], extra = {}) {
    return { name, type: "result", children, edges: rowCounts.map((rowCount) => ({ rowCount })), properties: [], cost: 0, ...extra };
}

test("plan layout places the root top-left, first sources level with their parent and later sources below everything so far", () => {
    const deep = node("Deep");
    const first = node("First", [deep], [5]);
    const second = node("Second");
    const root = node("Root", [first, second], [1000, 0]);
    const result = layout.layoutPlan(root, measure);

    const byName = new Map(result.nodes.map((n) => [n.node.name, n]));
    const r = byName.get("Root").bounds;
    assert.deepEqual([r.x, r.y], [32, 32]);
    assert.equal(r.width, 40);
    assert.equal(r.height, 32 + 10 + 16);

    const f = byName.get("First").bounds;
    assert.equal(f.x, r.x + r.width + 32);
    assert.equal(f.y, r.y, "First source stays on the same level");
    const d = byName.get("Deep").bounds;
    assert.equal(d.y, r.y);
    const s = byName.get("Second").bounds;
    assert.equal(s.x, f.x);
    assert.equal(s.y, r.y + r.height + 32, "Later sources move below the deepest node laid out so far");

    assert.equal(byName.get("First").parent, byName.get("Root"));
    assert.deepEqual([...byName.get("Root").children.map((c) => c.node.name)], ["First", "Second"]);
    assert.equal(result.width, d.x + d.width + 32);
    assert.equal(result.height, s.y + s.height + 32);

    const [toFirst, toDeep, toSecond] = result.edges;
    assert.equal(toFirst.target.node.name, "Root");
    assert.equal(toFirst.source.node.name, "First");
    assert.equal(toFirst.width, 3, "Edge width is log10 of the row count");
    assert.equal(toDeep.width, 1);
    assert.equal(toSecond.width, 1, "Empty edges still have a visible width");

    // Edges join the right of the parent icon, spaced evenly down it
    const rootIcon = byName.get("Root").icon;
    assert.equal(rootIcon.x, r.x + 4, "Icon is centred over the label");
    assert.deepEqual({ ...toFirst.end }, { x: rootIcon.x + 32, y: rootIcon.y + 10 });
    assert.deepEqual({ ...toSecond.end }, { x: rootIcon.x + 32, y: rootIcon.y + 20 });
    assert.equal(toFirst.start.y, toFirst.end.y, "Level edges are straight");
    assert.equal(layout.getEdgePath(toFirst), `M ${toFirst.start.x} ${toFirst.start.y} H ${toFirst.end.x + 3}`);
    const secondIcon = byName.get("Second").icon;
    assert.equal(toSecond.start.y, secondIcon.y + 16, "Lower edges start from the middle of the source icon");
    assert.match(layout.getEdgePath(toSecond), /^M \d+ \d+ H \d+ V \d+ H \d+$/);
});

test("sibling subtrees never overlap even when an earlier sibling has deep descendants", () => {
    const root = node("Root", [
        node("A", [node("A1"), node("A2"), node("A3")]),
        node("B", [node("B1"), node("B2")]),
        node("C"),
    ]);
    const { nodes } = layout.layoutPlan(root, measure);
    for (const a of nodes) {
        for (const b of nodes) {
            if (a === b) continue;
            const overlap = a.bounds.x < b.bounds.x + b.bounds.width && b.bounds.x < a.bounds.x + a.bounds.width
                && a.bounds.y < b.bounds.y + b.bounds.height && b.bounds.y < a.bounds.y + a.bounds.height;
            assert.equal(overlap, false, `${a.node.name} overlaps ${b.node.name}`);
        }
    }
});

test("executed plans include the cost and duration in the node labels", () => {
    const estimated = node("Select\r\naccount");
    assert.equal(layout.isExecutedPlan(estimated), false);
    assert.equal(layout.getNodeLabel(estimated, false), "Select\naccount");

    const executed = node("FetchXML Query", [], [], { cost: 0.25, costDisplayString: "25%", elapsedTimeInMs: 1234 });
    assert.equal(layout.isExecutedPlan(executed), true);
    const label = layout.getNodeLabel(executed, true).split("\n");
    assert.equal(label[0], "FetchXML Query");
    assert.match(label[1], /^Cost: 25\s?%$/);
    assert.match(label[2], /^1[.,]234s$/);
    assert.equal(layout.layoutPlan(executed, measure).root.bounds.height, 32 + 10 + 3 * 16);
});

test("node images use the SQL 4 CDS node type reported by the language server", () => {
    const available = (n) => images.has(n);
    for (const nodeType of ["FetchXmlScan", "MetadataQueryNode", "SelectNode", "HashJoinNode", "DistinctNode", "HashMatchAggregateNode", "ExecuteAsNode", "XmlWriterNode"]) {
        assert.equal(images.has(nodeType), true, `${nodeType}.ico exists in MarkMpn.Sql4Cds.Controls`);
        assert.equal(getNodeImageName({ nodeType }, available), nodeType);
    }
});

test("FetchXML Scan nodes expose the full FetchXML for opening in the XML editor", () => {
    const fetchXml = "<fetch>\n  <entity name=\"account\" />\n</fetch>";
    assert.equal(getFetchXmlQuery(node("FetchXML Query", [], [], {
        nodeType: "FetchXmlScan",
        properties: [{ name: "FetchXML", value: fetchXml }],
    })), fetchXml);
    assert.equal(getFetchXmlQuery(node("Other", [], [], {
        nodeType: "SelectNode",
        properties: [{ name: "FetchXML", value: fetchXml }],
    })), null);
    assert.equal(getFetchXmlQuery(node("FetchXML Query", [], [], {
        nodeType: "FetchXmlScan",
        properties: [{ name: "FetchXML", value: null }],
    })), null);
});

test("double-clicking a FetchXML Scan node opens its full query", () => {
    class Element {
        constructor() {
            this.children = [];
            this.events = {};
            this.style = {};
            this.attributes = {};
            this.classList = { add() {}, remove() {} };
        }
        append(...children) { this.children.push(...children); }
        appendChild(child) { this.children.push(child); }
        addEventListener(name, callback) { this.events[name] = callback; }
        setAttribute(name, value) { this.attributes[name] = value; }
    }
    const document = {
        createElement: () => new Element(),
        createElementNS: () => new Element(),
    };
    const { ExecutionPlanView } = load("executionPlan/executionPlanView.ts", {
        "../resultsProtocol": { BadgeType: { Warning: 0, CriticalWarning: 1 } },
        "./layout": layout,
    }, { document });
    const fetchXml = "<fetch>\n  <entity name=\"account\" />\n</fetch>";
    const opened = [];
    const view = new ExecutionPlanView(() => undefined, () => {}, measure, (query) => opened.push(query));
    view.addPlan({
        root: node("FetchXML Query", [], [], {
            nodeType: "FetchXmlScan",
            properties: [{ name: "FetchXML", value: fetchXml, showInTooltip: true }],
        }),
    });

    const findNode = (element) => element.className === "plan-node"
        ? element
        : element.children.map(findNode).find(Boolean);
    findNode(view.element).events.dblclick();

    assert.deepEqual(opened, [fetchXml]);
});

test("node images are substituted and fall back like the WinForms plan view", () => {
    const available = (n) => images.has(n);
    assert.equal(getNodeImageName({ nodeType: "AdaptiveIndexSpoolNode" }, available), "IndexSpoolNode");
    assert.equal(getNodeImageName({ nodeType: "OpenJsonNode" }, available), "ExecuteMessageNode");
    assert.equal(getNodeImageName({ nodeType: "SystemFunctionNode" }, available), "ExecuteMessageNode");
    assert.equal(getNodeImageName({ nodeType: "WindowSpoolNode" }, available), "TableSpoolNode");
    assert.equal(getNodeImageName({ nodeType: "UnknownNode" }, available), "FallbackNode");
    assert.equal(getNodeImageName({ nodeType: "UnknownCursorNode" }, available), "CursorFallbackNode");
    assert.equal(getNodeImageName({}, available), "FallbackNode");
    assert.equal(images.has("FallbackNode") && images.has("CursorFallbackNode"), true);
});
