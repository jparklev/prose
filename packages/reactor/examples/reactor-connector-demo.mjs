// Really using Reactor — the reactive-update loop, done right via an ARMED
// connector (the blessed ingress path). Boot the surprise-cost graph with a
// static connector on the `signals` gateway, then poll a growing feed: each NEW
// arrival (distinct id) stages into signals::ingress, moves the gateway's
// input_fingerprints → gateway re-renders reading the staged signal → digest
// re-renders grounded in it. A poll with NO new arrival is calm (cursor dedups).
//
//   node reactor-connector-demo.mjs <project-dir> <state-dir>

import { reactor } from "@openprose/reactor";
import { createCodexRenderBackend, createCodexCompileBackend } from "@openprose/reactor/adapters";
import { mkdirSync, writeFileSync } from "node:fs";

const project = process.argv[2];
const state = process.argv[3];
if (!project || !state) { console.error("usage: node reactor-connector-demo.mjs <project> <state>"); process.exit(2); }

let feed = []; // the connector's source: a growing JSON array keyed by `id`
// fetch must be SYNCHRONOUS and return a plain JSON payload — the poll adapter
// clones the return value through canonical adapter-JSON (which rejects a Promise
// / non-plain object), so `async () => feed` would throw on the Promise.
const signalsConnector = { node: "signals", source_id: "signals", fetch: () => feed };

// THE #12 FIX: a truth projection. The facade defaults projectTruthFor to
// EMPTY_PROJECTION (() => {}), so EVERY node's canonicalizer reduces an empty
// truth → its @atomic fingerprint is the constant sha256("null") → it never
// "moves" → no update ever propagates. We supply a real projection that parses
// the render's STRUCTURED truth out of whatever .json file it wrote (the Codex
// render names it signal.json / world-model.json), so the fingerprint tracks the
// content and a changed truth wakes downstream subscribers.
const _dec = new TextDecoder();
function projectTruth(files) {
  const names = Object.keys(files).filter((k) => k.toLowerCase().endsWith(".json"));
  const read = (name) => { try { return JSON.parse(_dec.decode(files[name])); } catch { return undefined; } };
  const prefer = names.find((n) => /world-model|truth/i.test(n)) ?? (names.length === 1 ? names[0] : undefined);
  if (prefer) { const v = read(prefer); if (v && typeof v === "object") return v; }
  const merged = {};
  for (const n of names) { const v = read(n); if (v && typeof v === "object") Object.assign(merged, v); }
  return merged;
}

const { reactor: r, pollConnectors } = await reactor(project, {
  directory: state,
  compile: { skipPostconditions: true, options: { compileBackend: createCodexCompileBackend({ sandboxMode: "read-only" }) } },
  // projectTruthFor (#12 fix): project each node's structured truth so its
  // canonicalizer fingerprint MOVES when the truth changes → updates propagate.
  render: { projectTruthFor: () => projectTruth },
  adapters: { renderBackend: createCodexRenderBackend({ sandboxMode: "workspace-write" }), connectors: [signalsConnector] },
});

const topo = r.topology?.topology;
if (topo) { mkdirSync(state + "/compile", { recursive: true }); writeFileSync(state + "/compile/topology.json", JSON.stringify(topo, null, 2)); }
const nodes = topo?.nodes?.map((n) => n.node) ?? [];
const digestNode = nodes.find((n) => /digest/i.test(n)) ?? "digest";
console.log(`[connector-demo] booted. edges=${topo?.edges?.length ?? 0} nodes=${nodes.join(",")}`);

const dec = new TextDecoder();
function digestTruth() {
  try {
    const f = r.store.read(digestNode)?.files ?? {};
    const k = f["world-model.json"] ? "world-model.json" : Object.keys(f)[0];
    return k ? dec.decode(f[k]) : "(no files)";
  } catch (e) { return "(read error: " + (e?.message ?? e) + ")"; }
}

let prior = r.ledger.all().length;
async function poll(label) {
  const before = prior;
  const res = await pollConnectors();
  const all = r.ledger.all();
  const fresh = all.slice(before).map((x) => `${x.node}:${x.status}`);
  prior = all.length;
  console.log(`\n[${label}]`);
  console.log(`  poll result: ${JSON.stringify(res).slice(0, 140)}`);
  console.log(`  new receipts: ${fresh.length ? fresh.join(", ") : "(none — calm)"}`);
  console.log(`  digest truth: ${digestTruth().slice(0, 200)}`);
}

console.log(`[connector-demo] initial digest truth: ${digestTruth().slice(0, 160)}`);
// Each arrival carries an explicit monotone `epoch` so the gateway can select
// the NEWEST from the accumulated ingress set (the #12 fix — surprise-cost's
// gateway maintains "the latest signal, epoch = which delivery produced it").
feed = [{ id: "s1", epoch: 1, headline: "all systems nominal" }];
await poll("UPDATE 1 · epoch 1: all systems nominal");
feed = [...feed, { id: "s2", epoch: 2, headline: "P1 incident: checkout latency breach (p99 1820ms)" }];
await poll("UPDATE 2 · epoch 2: P1 incident — checkout latency breach");
feed = [...feed, { id: "s3", epoch: 3, headline: "incident mitigated — latency recovering (p99 240ms)" }];
await poll("UPDATE 3 · epoch 3: incident mitigated — recovering");
await poll("REPEAT poll · no new arrival (expect calm — cost scales with surprise)");
console.log(`\n[connector-demo] DONE. total receipts=${r.ledger.all().length} · open: reactor-devtools ${state}`);
process.exit(0);
