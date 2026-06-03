---
name: digest
kind: responsibility
---

# Digest

A standing service-health brief: it restates the current signal as a one-line
status, re-rendered only when the upstream signal materially moves.

### Requires

- The `signals` gateway's maintained truth on its atomic facet — the current
  `{ headline, epoch }`. The digest reads the upstream `headline` by reference.

### Maintains

The current health brief, as this responsibility's maintained truth:

- `brief`: a one-line health brief that restates the current upstream `headline`.
- `source_epoch`: the gateway `epoch` this brief was derived from.

The render reads its prior truth by reference and self-polices these
postconditions before signing (no separate judge beat):

- `brief` restates the CURRENT upstream `headline` (it is never stale);
- `source_epoch` equals the gateway `epoch` the brief was derived from.

### Execution

1. Read the upstream `signals` truth by reference (`headline`, `epoch`).
2. Maintain the new truth: `{ brief: "health: " + headline, source_epoch: epoch }`.

### Continuity

input-driven: the digest re-renders when its required upstream truth moves. A
re-wake that finds no material move writes an unmoved fingerprint and stops
(a skipped receipt). Cost scales with surprise.
