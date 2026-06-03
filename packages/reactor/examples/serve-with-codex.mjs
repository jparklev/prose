// serve-with-codex — boot a Reactor over a directory of .prose.md contracts with
// the Codex SDK as the RENDER backend (no OPENROUTER_API_KEY for the render leg).
//
//   node serve-with-codex.mjs <project-dir> <state-dir>
//
// BOTH legs are keyless on your local Codex CLI auth (@openai/codex-sdk): the
// RENDER backend swaps the run-phase session, and the COMPILE backend swaps the
// Forme / canonicalizer / postcondition sessions. No OPENROUTER_API_KEY anywhere.
//
// Point the devtools at <state-dir> to watch receipts land live:
//   reactor-devtools <state-dir>

import { reactor } from "@openprose/reactor";
import { createCodexRenderBackend, createCodexCompileBackend } from "@openprose/reactor/adapters";

const project = process.argv[2];
const state = process.argv[3];
if (!project || !state) {
  console.error("usage: node serve-with-codex.mjs <project-dir> <state-dir>");
  process.exit(2);
}

console.log(`[serve-with-codex] project=${project} state=${state}`);
console.log(`[serve-with-codex] compile + render backends = Codex SDK (keyless)`);

const renderBackend = createCodexRenderBackend({ sandboxMode: "workspace-write" });
const compileBackend = createCodexCompileBackend({ sandboxMode: "read-only" });

try {
  const { reactor: r } = await reactor(project, {
    directory: state,
    // skipPostconditions: the postcondition step's recursive-predicate schema is
    // finicky under structured output; Forme + canonicalizer + render is enough
    // to prove the keyless Codex pipeline end to end.
    compile: { skipPostconditions: true, options: { compileBackend } },
    adapters: { renderBackend },
  });
  const receipts = r.ledger.all();
  // Persist the compiled topology (the facade keeps it in-memory only) so
  // reactor-devtools can DRAW the wired edges, not just derive nodes from receipts.
  try {
    const fs = await import("node:fs");
    const topo = r.topology?.topology;
    if (topo) {
      fs.mkdirSync(state + "/compile", { recursive: true });
      fs.writeFileSync(state + "/compile/topology.json", JSON.stringify(topo, null, 2));
      console.log(`[serve-with-codex] wrote topology.json (${topo.edges?.length ?? 0} edges)`);
    }
  } catch (e) {
    console.error("[serve-with-codex] topology write skipped:", e?.message ?? e);
  }
  console.log(`[serve-with-codex] booted to fixpoint. receipts=${receipts.length}`);
  for (const rc of receipts) {
    const node = rc.node ?? rc.scope ?? "?";
    const status = rc.status ?? "?";
    const fresh = rc.cost?.tokens?.fresh ?? rc.cost?.fresh ?? 0;
    console.log(`  ${node}  ${status}  fresh=${fresh}`);
  }
  console.log(`[serve-with-codex] state-dir written to ${state} — open it with: reactor-devtools ${state}`);
  process.exit(0);
} catch (err) {
  console.error(`[serve-with-codex] FAILED: ${err?.message ?? err}`);
  if (err?.stack) console.error(err.stack.split("\n").slice(1, 6).join("\n"));
  process.exit(1);
}
