// The Codex-SDK render backend — an alternate body executor for the
// `RenderBackend` injection seam (API-ANALYSIS §5.4), driving the OpenAI Codex
// SDK (`@openai/codex-sdk`) instead of the default `@openai/agents` + provider.
//
// Why this exists: a render is "one bounded model session". The default backend
// reaches a token-billed provider (OpenRouter) and needs `OPENROUTER_API_KEY`.
// This backend rides the local Codex CLI auth instead — no per-token key — and
// is the SANCTIONED programmatic surface (the Codex SDK is published expressly
// for "build Codex into your own tools/workflows/CI"). It REUSES the harness's
// instruction-composition / working-dir prep / harvest / cost machinery; it owns
// only the one session, exactly like every other `RenderBackend`.
//
// Peer isolation: this module has NO static import of `@openai/codex-sdk` and
// types the SDK structurally, so the keyless core never loads the peer. The SDK
// is `import()`-ed lazily on first `runSession`. Token usage maps into the same
// `RenderUsage` the cost machinery prices (cached tokens → the `cached_tokens`
// key the memo accounting reads, so fresh/reused still works).
//
// Output bridge: the harness `outputType` is an `@openai/agents` schema, not a
// JSON Schema, so we do NOT hand it to Codex's `outputSchema`. Instead we append
// a small instruction to emit the done/failed signal as a JSON object and parse
// the turn's `finalResponse`. A consumer that has a real JSON Schema can pass it
// via `outputSchema` for hard enforcement.

import type { RenderBackend, RenderSessionRequest, RenderSessionOutput } from "../agent-render/render-backend";
import type { RenderOutputSignal } from "../agent-render/output-schema";
import type { RenderUsage } from "../agent-render/cost";
import type { CompileBackend, CompileSessionRequest, CompileSessionOutput } from "../agent-compile/session";

/** Codex SDK sandbox presets (mirrors `@openai/codex-sdk`'s `SandboxMode`). */
export type CodexSandboxMode = "read-only" | "workspace-write" | "danger-full-access";

export interface CodexRenderBackendConfig {
  /** Model id to run (e.g. "gpt-5.4-codex"). Default: the request's model, else the SDK default. */
  readonly model?: string;
  /** Sandbox preset for the render session. Default "workspace-write" (the node writes its world-model). */
  readonly sandboxMode?: CodexSandboxMode;
  /** Reasoning effort, when the model supports it. */
  readonly modelReasoningEffort?: "minimal" | "low" | "medium" | "high";
  /** Skip Codex's git-repo check (a per-node working dir is usually not a repo). Default true. */
  readonly skipGitRepoCheck?: boolean;
  /** A JSON Schema for the done signal, if you have one — handed to Codex's `outputSchema`. */
  readonly outputSchema?: unknown;
  /** Inject a pre-constructed Codex client (tests / a configured instance). */
  readonly codex?: CodexLike;
}

// --- Structural ports over @openai/codex-sdk (no static import) -------------
interface CodexUsageLike {
  readonly input_tokens?: number;
  readonly cached_input_tokens?: number;
  readonly output_tokens?: number;
  readonly reasoning_output_tokens?: number;
}
interface CodexTurnLike {
  readonly finalResponse: string;
  readonly usage: CodexUsageLike | null;
}
interface CodexThreadLike {
  run(input: string, opts?: { outputSchema?: unknown; signal?: AbortSignal }): Promise<CodexTurnLike>;
}
interface CodexLike {
  startThread(opts?: Record<string, unknown>): CodexThreadLike;
}

const ZERO_USAGE: RenderUsage = { inputTokens: 0, outputTokens: 0, totalTokens: 0 };

const SIGNAL_INSTRUCTION =
  "\n\nWhen the world-model is up to date, end your turn by emitting ONLY a JSON " +
  'object as your final message: {"status":"done","summary":"<one line>"} on ' +
  'success, or {"status":"failed","reason":"<why>"} if you cannot satisfy the ' +
  "postconditions (the prior truth then stands).";

/**
 * Build a {@link RenderBackend} that runs each render as one Codex SDK turn.
 * Inject via `reactor("./project", { adapters: { renderBackend: createCodexRenderBackend() } })`.
 */
// A genuine dynamic ESM import that tsc's CommonJS lowering won't rewrite to
// `require()` — @openai/codex-sdk is ESM-only (its exports map has no "require"
// condition), so a require() resolves to "No exports main defined". `new Function`
// hides the import() from the compiler so it stays a real ESM import at runtime.
const esmImport = new Function("s", "return import(s)") as (s: string) => Promise<unknown>;

// Lazy, once — a keyless build that never renders/compiles never loads the peer.
function codexGetter(injected?: CodexLike): () => Promise<CodexLike> {
  let p: Promise<CodexLike> | undefined;
  return async () => {
    if (injected) return injected;
    if (!p) p = esmImport("@openai/codex-sdk").then((m) => new (m as { Codex: new () => CodexLike }).Codex());
    return p;
  };
}

export function createCodexRenderBackend(config: CodexRenderBackendConfig = {}): RenderBackend {
  const getCodex = codexGetter(config.codex);

  return {
    async runSession(req: RenderSessionRequest): Promise<RenderSessionOutput> {
      let codex: CodexLike;
      try {
        codex = await getCodex();
      } catch (err) {
        return fail(`@openai/codex-sdk is not available: ${msg(err)}`);
      }
      // Use config.model if set, else let Codex pick the account default. We do
      // NOT inherit req.model — that's the OpenRouter render-model (e.g.
      // google/gemini-3.5-flash), which Codex rejects on a ChatGPT account.
      const model = config.model;
      const threadOpts: Record<string, unknown> = {
        sandboxMode: config.sandboxMode ?? "workspace-write",
        skipGitRepoCheck: config.skipGitRepoCheck ?? true,
      };
      if (model !== undefined) threadOpts["model"] = model;
      if (req.context.workingDir !== undefined) threadOpts["workingDirectory"] = req.context.workingDir;
      if (config.modelReasoningEffort !== undefined) threadOpts["modelReasoningEffort"] = config.modelReasoningEffort;

      // The harness composes instructions (SKILL + contract) + a short pointer
      // input; concatenate and request the structured done/failed signal.
      const prompt = `${req.instructions}\n\n${req.input}${SIGNAL_INSTRUCTION}`;
      const runOpts: { outputSchema?: unknown; signal?: AbortSignal } = {};
      if (config.outputSchema !== undefined) runOpts.outputSchema = config.outputSchema;
      if (req.signal !== undefined) runOpts.signal = req.signal;

      let turn: CodexTurnLike;
      try {
        turn = await codex.startThread(threadOpts).run(prompt, runOpts);
      } catch (err) {
        return fail(`codex turn threw: ${msg(err)}`);
      }
      return { signal: parseSignal(turn.finalResponse), usage: mapUsage(turn.usage) };
    },
  };
}

const compileInstruction = (step: string): string =>
  step === "forme"
    ? `\n\nEmit ONLY the JSON artifact (no prose). The "matches" MUST wire EVERY subscriber's "### Requires" facet to the producing node's "### Maintains" facet — never omit an edge; an unwired subscription is a broken compile.`
    : `\n\nEmit ONLY the JSON artifact for the "${step}" compile step as your final message — no prose.`;

// Derive a JSON Schema from the step's zod outputType (zod v4 `z.toJSONSchema`),
// so Codex can hard-enforce the artifact shape. Best-effort: returns undefined if
// the outputType isn't a zod schema or the conversion isn't representable.
async function jsonSchemaFor(outputType: unknown): Promise<unknown | undefined> {
  const ot = outputType as { safeParse?: unknown } | null;
  if (!ot || typeof ot.safeParse !== "function") return undefined;
  try {
    const z = (await import("zod")) as { toJSONSchema?: (s: unknown) => unknown };
    return typeof z.toJSONSchema === "function" ? z.toJSONSchema(outputType) : undefined;
  } catch {
    return undefined;
  }
}

// Validate the parsed artifact against the step's zod schema. On success returns
// the coerced value; on a SOFT mismatch falls back to the raw parsed artifact
// (lenient — the outputSchema already enforced shape at generation, and the
// downstream deterministic lowering catches a truly-broken artifact). Only a
// TOTAL parse failure (`parsed === undefined`) surfaces a failed compile.
function zodValidate(outputType: unknown, parsed: unknown): unknown {
  if (parsed === undefined || parsed === null) return undefined;
  const ot = outputType as { safeParse?: (v: unknown) => { success: boolean; data?: unknown } } | null;
  if (ot && typeof ot.safeParse === "function") {
    const r = ot.safeParse(parsed);
    // On success return the COERCED data (zod fills defaults). On a SOFT mismatch
    // keep the raw parsed (the LLM's compile output is often approximate but
    // usable, and the deterministic lowering is the real validator) — strict
    // rejection here over-fails the canonicalizer. The lowering-crash vector
    // (a missing array `.map`ed over) is handled by `guardArtifact` below.
    return r.success ? r.data : parsed;
  }
  return parsed;
}

// Crash-guard: the deterministic lowering maps over arrays in the compile
// artifact. An approximate LLM artifact may omit one (run-9: toCanonicalizationSpec
// `.map` on undefined). Coerce the arrays the lowering needs so a malformed
// artifact degrades to a usable (possibly empty) one instead of crashing boot.
function guardArtifact(step: string, output: unknown): unknown {
  if (!output || typeof output !== "object") return output;
  const o = output as Record<string, unknown>;
  if (step === "forme") {
    if (!Array.isArray(o["nodes"])) o["nodes"] = [];
    if (!Array.isArray(o["matches"])) o["matches"] = [];
  } else if (step === "canonicalizer") {
    if (!Array.isArray(o["fields"])) o["fields"] = [];
    if (!Array.isArray(o["facets"])) o["facets"] = [];
  }
  return o;
}

/**
 * Build a {@link CompileBackend} that runs each compile step as one Codex SDK
 * turn — the keyless COMPILE leg (the compile-phase analogue of the render
 * backend). Sandbox defaults to read-only: a compile step reads the contract set
 * and emits a structured artifact; it writes no world-model. Inject via
 * `reactor("./proj", { compile: { backend: createCodexCompileBackend() } })`.
 */
export function createCodexCompileBackend(config: CodexRenderBackendConfig = {}): CompileBackend {
  const getCodex = codexGetter(config.codex);
  return {
    async runSession(req: CompileSessionRequest): Promise<CompileSessionOutput> {
      const codex = await getCodex();
      // Use config.model if set, else let Codex pick the account default. We do
      // NOT inherit req.model — that's the OpenRouter render-model (e.g.
      // google/gemini-3.5-flash), which Codex rejects on a ChatGPT account.
      const model = config.model;
      const threadOpts: Record<string, unknown> = {
        sandboxMode: config.sandboxMode ?? "read-only",
        skipGitRepoCheck: config.skipGitRepoCheck ?? true,
      };
      if (model !== undefined) threadOpts["model"] = model;
      if (config.modelReasoningEffort !== undefined) threadOpts["modelReasoningEffort"] = config.modelReasoningEffort;
      const prompt = `${req.instructions}\n\n${req.input}${compileInstruction(req.step)}`;
      const schema = config.outputSchema ?? (await jsonSchemaFor(req.outputType));
      const baseOpts: { signal?: AbortSignal } = {};
      if (req.signal !== undefined) baseOpts.signal = req.signal;
      // Hard-enforce the artifact shape via outputSchema; if Codex rejects the
      // schema (e.g. not strict-mode-compatible), retry once free-form.
      let turn;
      try {
        turn = await codex.startThread(threadOpts).run(prompt, schema !== undefined ? { ...baseOpts, outputSchema: schema } : baseOpts);
      } catch (err) {
        if (schema === undefined) throw err;
        turn = await codex.startThread(threadOpts).run(prompt, baseOpts);
      }
      // Optional debug capture of the raw turn (REACTOR_CODEX_DEBUG=<path>).
      if (process.env["REACTOR_CODEX_DEBUG"]) {
        try {
          const fs = (await import("node:fs")) as typeof import("node:fs");
          fs.appendFileSync(process.env["REACTOR_CODEX_DEBUG"], JSON.stringify({ step: req.step, schemaEnforced: schema !== undefined, finalResponse: turn.finalResponse }) + "\n");
        } catch { /* ignore debug write errors */ }
      }
      // Validate (lenient) then crash-guard the arrays the lowering maps over.
      const output = guardArtifact(req.step, zodValidate(req.outputType, parseJsonArtifact(turn.finalResponse)));
      return { output, usage: mapUsage(turn.usage) };
    },
  };
}

/** Parse a compile turn's final message into the structured artifact (object or
 *  array). `undefined` ⇒ runCompileSession throws (a failed compile). */
export function parseJsonArtifact(text: string): unknown {
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  let raw = fence && fence[1] ? fence[1].trim() : text.trim();
  if (!(raw.startsWith("{") || raw.startsWith("["))) {
    const o = raw.indexOf("{");
    const a = raw.indexOf("[");
    const start = o < 0 ? a : a < 0 ? o : Math.min(o, a);
    const endO = raw.lastIndexOf("}");
    const endA = raw.lastIndexOf("]");
    const end = Math.max(endO, endA);
    if (start < 0 || end <= start) return undefined;
    raw = raw.slice(start, end + 1);
  }
  try {
    return JSON.parse(raw);
  } catch {
    return undefined;
  }
}

function fail(reason: string): RenderSessionOutput {
  return { signal: { status: "failed", reason } as unknown as RenderOutputSignal, usage: ZERO_USAGE };
}
function msg(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function mapUsage(u: CodexUsageLike | null): RenderUsage {
  if (!u) return ZERO_USAGE;
  const inputTokens = u.input_tokens ?? 0;
  const outputTokens = (u.output_tokens ?? 0) + (u.reasoning_output_tokens ?? 0);
  const cached = u.cached_input_tokens ?? 0;
  const base: RenderUsage = { inputTokens, outputTokens, totalTokens: inputTokens + outputTokens };
  // Surface cached tokens under the key the cost machinery reads, so the memo
  // fresh/reused split (and the API-equivalent $ rollup) stays meaningful.
  return cached > 0 ? { ...base, inputTokensDetails: [{ cached_tokens: cached }] } : base;
}

/** Parse the turn's final message into a done/failed signal. `undefined` ⇒ the
 *  harness treats the session as failed and nothing commits (prior truth stands). */
export function parseSignal(text: string): RenderOutputSignal | undefined {
  const json = extractJson(text);
  if (json === null) return undefined;
  try {
    const v = JSON.parse(json) as Record<string, unknown>;
    if (v && (v["status"] === "done" || v["status"] === "failed")) return v as unknown as RenderOutputSignal;
    // A structured object without an explicit status → treat as a done payload.
    return { status: "done", ...v } as unknown as RenderOutputSignal;
  } catch {
    return undefined;
  }
}

function extractJson(text: string): string | null {
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence && fence[1]) return fence[1].trim();
  const t = text.trim();
  if (t.startsWith("{") && t.endsWith("}")) return t;
  const first = t.indexOf("{");
  const last = t.lastIndexOf("}");
  if (first >= 0 && last > first) return t.slice(first, last + 1);
  return null;
}
