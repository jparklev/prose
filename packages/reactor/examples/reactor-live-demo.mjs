// Really using Reactor: a reactive world-model maintenance loop on the keyless
// Codex backends. Boot the surprise-cost graph, then ingest a SEQUENCE of signals
// and watch the maintained truth track them — and memo-SKIP when nothing moved.
//
//   node reactor-live-demo.mjs <project-dir> <state-dir>
//
// Drives the real thesis: cost scales with surprise. A changed signal re-renders
// digest (grounded in the new headline); a byte-identical re-ingest memo-skips.

import { reactor } from "@openprose/reactor";
import { createCodexRenderBackend, createCodexCompileBackend } from "@openprose/reactor/adapters";
import { mkdirSync, writeFileSync } from "node:fs";

const project = process.argv[2];
const state = process.argv[3];
if (!project || !state) { console.error("usage: node reactor-live-demo.mjs <project> <state>"); process.exit(2); }

const { reactor: r } = await reactor(project, {
  directory: state,
  compile: { skipPostconditions: true, options: { compileBackend: createCodexCompileBackend({ sandboxMode: "read-only" }) } },
  adapters: { renderBackend: createCodexRenderBackend({ sandboxMode: "workspace-write" }) },
});

// Persist topology so the devtools viewer can draw the wired edge.
const topo = r.topology?.topology;
if (topo) { mkdirSync(state + "/compile", { recursive: true }); writeFileSync(state + "/compile/topology.json", JSON.stringify(topo, null, 2)); }

const nodes = topo?.nodes?.map((n) => n.node) ?? [];
const signalsNode = nodes.find((n) => /signal/i.test(n)) ?? "signals";
const digestNode = nodes.find((n) => /digest/i.test(n)) ?? "digest";
console.log(`[live-demo] booted. signals=${signalsNode} digest=${digestNode} · edges=${topo?.edges?.length ?? 0}`);

const dec = new TextDecoder();
const enc = new TextEncoder();
function digestTruth() {
  try {
    const read = r.store.read(digestNode);
    const files = read?.files ?? {};
    const key = files["world-model.json"] ? "world-model.json" : Object.keys(files)[0];
    return key ? dec.decode(files[key]) : "(no files)";
  } catch (e) { return "(read error: " + (e?.message ?? e) + ")"; }
}

let prior = r.ledger.all().length;
async function ingest(label, headline) {
  console.log(`\n[live-demo] ${label} → ingest signal: "${headline}"`);
  await r.ingest(signalsNode, { wake: { source: "external", refs: [] }, data: { "signal.txt": enc.encode(headline) } });
  const all = r.ledger.all();
  const fresh = all.slice(prior).map((x) => `${x.node}:${x.status}`);
  prior = all.length;
  console.log(`  new receipts: ${fresh.length ? fresh.join(", ") : "(none — memo-skip)"}`);
  console.log(`  digest truth: ${digestTruth().slice(0, 140)}`);
}

console.log(`[live-demo] initial digest truth: ${digestTruth().slice(0, 140)}`);
await ingest("UPDATE 1", "all systems nominal");
await ingest("UPDATE 2", "P1 incident: checkout latency breach (p99 1820ms)");
await ingest("REPEAT 2 (memo-skip expected)", "P1 incident: checkout latency breach (p99 1820ms)");
console.log(`\n[live-demo] DONE. total receipts=${r.ledger.all().length} · open: reactor-devtools ${state}`);
process.exit(0);
