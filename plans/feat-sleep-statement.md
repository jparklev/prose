# Feature: Add `sleep` Statement to Prose VM

**Type:** Improvement - Developer Experience
**Date:** 2026-01-21
**Status:** Planned

---

## Summary

Add a `sleep` statement to the prose language that pauses execution for a specified duration. This enables developers to implement rate limiting, polling intervals, and timing-based workflows without workarounds.

**Syntax:** `sleep <duration>` where duration uses time unit suffixes (`ms`, `s`, `m`, `h`)

---

## Problem / Motivation

The prose VM currently lacks an explicit mechanism to pause execution for a fixed time period. Developers face these challenges:

1. **Rate Limiting:** When calling external APIs with rate limits, there's no clean way to add delays between calls
2. **Polling Intervals:** Loops that poll for status changes cannot easily add consistent delays between iterations
3. **External Process Timing:** No way to wait for external processes to stabilize before continuing
4. **Workarounds Required:** Developers must rely on `backoff` properties (only available on retry) or implicit Task latency

### Current State

- `backoff: exponential|linear|none` exists but only applies to `retry:` scenarios
- `timeout: 120s` appears in examples but isn't a general-purpose delay
- No grammar rule for standalone delay/sleep statements

---

## Proposed Solution

### Syntax

```prose
sleep 5s           # Sleep for 5 seconds
sleep 100ms        # Sleep for 100 milliseconds
sleep 2m           # Sleep for 2 minutes
sleep 1h           # Sleep for 1 hour
```

### Supported Time Units

| Unit | Meaning      | Example  |
|------|--------------|----------|
| `ms` | Milliseconds | `500ms`  |
| `s`  | Seconds      | `5s`     |
| `m`  | Minutes      | `2m`     |
| `h`  | Hours        | `1h`     |

### Grammar Addition

In `compiler.md` line 2854-2858, add `sleepStatement` to the statement production:

```
statement   → useStatement | inputDecl | agentDef | session | resumeStmt
            | letBinding | constBinding | assignment | outputBinding
            | parallelBlock | repeatBlock | forEachBlock | loopBlock
            | tryBlock | choiceBlock | ifStatement | doBlock | blockDef
            | throwStatement | sleepStatement | comment

sleepStatement → "sleep" duration
duration       → NUMBER timeUnit
timeUnit       → "ms" | "s" | "m" | "h"
```

### Execution Semantics

In `prose.md`, add sleep to the statement execution section:

```
If sleepStatement:
  1. Parse duration value and unit
  2. Convert to milliseconds
  3. Pause execution for the specified duration
  4. Continue to next statement
```

The VM implementation note: Since the prose VM runs on an LLM session, "sleeping" means the orchestrating system should delay before continuing. In practice this translates to a timer/delay in the host environment (e.g., Claude Code's Task tool could use `sleep` bash commands or timer APIs).

---

## Technical Considerations

### 1. Files to Modify

| File | Changes |
|------|---------|
| `skills/open-prose/compiler.md` | Add grammar rule (line ~2858), validation rules, and syntax section |
| `skills/open-prose/prose.md` | Add execution semantics (line ~330 grammar, ~500 execution) |
| `skills/open-prose/help.md` | Add to syntax at-a-glance section |

### 2. Grammar Integration Points

**compiler.md:2853-2858** - Add `sleepStatement` to statement production:
```
statement   → ... | sleepStatement | comment
```

**compiler.md (new section ~line 1520)** - Add dedicated Sleep Statement section:
```markdown
## Sleep Statement

Pauses execution for a specified duration.

### Syntax
sleep <duration>

### Duration Format
<number><unit>
- ms: milliseconds
- s: seconds
- m: minutes
- h: hours

### Examples
sleep 5s      # Wait 5 seconds
sleep 100ms   # Wait 100 milliseconds
sleep 2m      # Wait 2 minutes
```

### 3. Validation Rules

| Check | Severity | Code | Message |
|-------|----------|------|---------|
| Invalid time unit | Error | E029 | `Invalid time unit '{unit}'. Use ms, s, m, or h` |
| Zero duration | Warning | W012 | `Sleep duration is 0, statement has no effect` |
| Very long duration (>1h) | Warning | W013 | `Sleep duration exceeds 1 hour, consider breaking into smaller waits` |
| Negative duration | Error | E030 | `Sleep duration must be positive` |

### 4. Implementation Pattern

Following the existing statement pattern from `compiler.md`:

1. **Grammar Rule** - Added to EBNF (line 2853-2858)
2. **Syntax Section** - New markdown section with examples
3. **Validation Table** - Error/warning codes
4. **Execution Semantics** - In `prose.md`

---

## Edge Cases

1. **Zero duration (`sleep 0s`)** - Should be valid but emit a warning; effectively a no-op
2. **Very large values (`sleep 999h`)** - Valid but warn; no hard maximum
3. **Fractional values (`sleep 1.5s`)** - Decision: Support or integer-only?
   - **Recommendation:** Support integers only initially; `sleep 1500ms` covers fractional seconds
4. **Sleep in parallel blocks** - Each branch sleeps independently; doesn't block siblings
5. **Sleep in loops** - Executes each iteration; total delay = iterations x sleep duration
6. **Interruption** - No interruption mechanism initially; sleep always completes

---

## Testing Approach

### 1. Syntax Validation Tests

```prose
# Valid syntax
sleep 1s
sleep 100ms
sleep 5m
sleep 1h

# Invalid syntax (should error)
sleep 5           # Missing unit
sleep 5x          # Invalid unit
sleep -5s         # Negative
sleep s           # Missing number
```

### 2. Example Program

Create `examples/50-sleep-statement.prose`:

```prose
# Demonstrates the sleep statement for rate limiting

session "Start process"
  prompt: "Begin the long-running task"

sleep 2s  # Wait for external process to initialize

session "Check status"
  prompt: "Verify the process started correctly"

# Rate-limited API calls
repeat 3:
  session "Call API"
    prompt: "Make API request"
  sleep 1s  # Respect rate limit
```

### 3. Integration Test Cases

| Test Case | Input | Expected |
|-----------|-------|----------|
| Basic sleep | `sleep 1s` | Pauses ~1 second |
| Milliseconds | `sleep 500ms` | Pauses ~500ms |
| In loop | `repeat 3: sleep 1s` | Total ~3 seconds |
| In parallel | `parallel: sleep 2s / sleep 1s` | Completes in ~2s (max branch) |
| Zero | `sleep 0s` | Warning, no delay |

---

## Acceptance Criteria

- [ ] Grammar rule added to `compiler.md` (sleepStatement production)
- [ ] Syntax section documented with examples
- [ ] Validation rules defined (E029, E030, W012, W013)
- [ ] Execution semantics added to `prose.md`
- [ ] Help documentation updated in `help.md`
- [ ] Example program created (`examples/50-sleep-statement.prose`)
- [ ] All time units work: `ms`, `s`, `m`, `h`
- [ ] Invalid syntax produces clear error messages

---

## Future Considerations (Out of Scope)

These are explicitly **not** part of this implementation:

- **Fractional durations** (`sleep 1.5s`) - Use milliseconds instead
- **Interruptible sleep** - No early wake mechanism
- **Variable durations** (`sleep delay_var`) - Fixed literals only initially
- **Named time constants** - No `sleep short` or `sleep rate_limit`

---

## References

- `skills/open-prose/compiler.md:2850-2953` - Full grammar specification
- `skills/open-prose/prose.md:320-420` - Condensed grammar and execution
- `skills/open-prose/examples/23-retry-with-backoff.prose` - Related timing patterns
- `skills/open-prose/guidance/patterns.md` - Existing timing patterns
