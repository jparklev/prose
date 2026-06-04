// Tests for the sideband INVARIANT evaluator — the `observability.json` sidecar
// the read-only devtools projection reads (and the reactor engine NEVER does).
//
// An invariant is a declared property of a node's maintained TRUTH ("typecheck
// revisions ≤ 8", "merge gate stays GREEN"). `evaluateInvariants` reads each
// node's LATEST committed truth out of its world-model (the structured-JSON
// convention) and returns the BREACHES — surfaced as `invariant-failed` surprises.
// Pure replay over the shipped monorepo-ci fixture (whose Merge Gate truth is
// `{ merge: "GREEN", typecheck: 10, … }`); no model key, no reactor.

import { strict as assert } from "node:assert";
import { test } from "node:test";
import { join } from "node:path";

import {
  openStateDir,
  buildSnapshot,
  evaluateInvariants,
  readObservabilitySidecar,
  type InvariantSpec,
} from "./index";

const FIXTURE = join(__dirname, "..", "..", "fixtures", "monorepo-ci");
const GATE = "gate.merge";

test("evaluateInvariants: a violated truth invariant becomes a breach (observed value + node's last frame)", () => {
  const opened = openStateDir(FIXTURE);
  const frames = buildSnapshot(opened).frames;
  const specs: InvariantSpec[] = [
    { id: "typecheck-budget", node: GATE, path: "typecheck", op: "lte", value: 8, label: "typecheck ≤ 8", severity: "warn" },
  ];
  const breaches = evaluateInvariants(opened, frames, specs);
  assert.equal(breaches.length, 1, "the typecheck=10 truth violates ≤ 8");
  const b = breaches[0]!;
  assert.equal(b.id, "typecheck-budget");
  assert.equal(b.node, GATE);
  assert.equal(b.observed, 10);
  assert.equal(b.severity, "warn");
  // frameIndex is the node's LAST committed frame (where the offending truth lives).
  const lastGateFrame = [...frames].reverse().find((f) => f.node === GATE)!;
  assert.equal(b.frameIndex, lastGateFrame.index);
  assert.match(b.reason, /violates ≤ 8/);
});

test("evaluateInvariants: a satisfied invariant produces NO breach", () => {
  const opened = openStateDir(FIXTURE);
  const frames = buildSnapshot(opened).frames;
  const specs: InvariantSpec[] = [
    { id: "merge-green", node: GATE, path: "merge", op: "eq", value: "GREEN" },
  ];
  assert.deepEqual(evaluateInvariants(opened, frames, specs), [], "merge === GREEN holds");
});

test("evaluateInvariants: an unknown node or missing truth path is skipped (not a breach)", () => {
  const opened = openStateDir(FIXTURE);
  const frames = buildSnapshot(opened).frames;
  const specs: InvariantSpec[] = [
    { id: "ghost", node: "no.such.node", path: "x", op: "gt", value: 0 },
    { id: "no-path", node: GATE, path: "nonexistent.deep.key", op: "gt", value: 0 },
  ];
  assert.deepEqual(evaluateInvariants(opened, frames, specs), [], "no node + no path → no breach");
});

test("readObservabilitySidecar: parses the shipped observability.json (ignores the _comment), defaults the sidecar to []", () => {
  const specs = readObservabilitySidecar(FIXTURE);
  assert.equal(specs.length, 2, "the two shipped invariants, _comment ignored");
  assert.ok(specs.some((s) => s.id === "typecheck-budget" && s.op === "lte"));
  assert.ok(specs.some((s) => s.id === "merge-green" && s.op === "eq"));
  // A dir with no sidecar yields the empty set (the common case — sidecar absent).
  assert.deepEqual(readObservabilitySidecar(join(__dirname, "..", "..", "fixtures", "masked-relay")), []);
});

test("evaluateInvariants: default specs read the sidecar — the fixture's one real breach surfaces", () => {
  const opened = openStateDir(FIXTURE);
  const frames = buildSnapshot(opened).frames;
  const breaches = evaluateInvariants(opened, frames); // no specs → reads observability.json
  assert.equal(breaches.length, 1, "typecheck-budget breaches; merge-green holds");
  assert.equal(breaches[0]!.id, "typecheck-budget");
});
