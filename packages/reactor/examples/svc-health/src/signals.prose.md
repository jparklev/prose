---
name: signals
kind: gateway
---

# Signals

The external ingress for service-health signals — the system's entry point. A
monitoring feed delivers the current signal from outside the graph; this gateway
normalizes it into the truth the `digest` responsibility subscribes to.

### Continuity: external-driven

The latest delivery is staged into this gateway's ingress as a JSON object
carrying `{ id, epoch, headline }`. A new delivery wakes this gateway.

### Receives

- The CURRENT incoming signal: a JSON object `{ id, epoch, headline }`.
  Provider: any upstream monitoring feed / webhook / poll. The ingress always
  presents the latest delivery (it is replaced on each new arrival, not
  accumulated).

### Maintains

The current service-health signal:

- `headline`: the one-line summary, copied VERBATIM from the incoming signal.
- `epoch`: the delivery marker, copied VERBATIM from the incoming signal.

**ADOPT-THE-INCOMING RULE:** your maintained truth MUST always reflect the
CURRENT incoming signal. On every render, read the incoming arrival and set
`headline` and `epoch` to exactly its values — REPLACING whatever you maintained
before. Never keep a stale prior signal: if the incoming `epoch` differs from
your prior `epoch`, you MUST update to the incoming one. The incoming signal is
the source of truth, not your previous truth.

This is a facet-less producer: it exposes its whole maintained truth as the
single atomic facet, which the digest subscribes to.

### Emits

- digest
