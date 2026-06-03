import { test } from "node:test";
import { strict as assert } from "node:assert";

import { createCodexRenderBackend, createCodexCompileBackend, parseSignal, parseJsonArtifact, type CodexRenderBackendConfig } from "./index";
import type { RenderSessionRequest } from "../agent-render/render-backend";
import type { CompileSessionRequest } from "../agent-compile/session";

type CodexCfg = NonNullable<CodexRenderBackendConfig["codex"]>;

// runSession only reads model / instructions / input / context.workingDir /
// signal, so a partial cast through `unknown` is enough.
function fakeReq(over: Record<string, unknown> = {}): RenderSessionRequest {
  return {
    node: "responsibility.x",
    instructions: "SKILL + contract",
    model: "gpt-5.4-codex",
    modelSettings: {},
    tools: [],
    outputType: undefined,
    input: "wake pointer",
    context: { node: "responsibility.x", store: {}, workingDir: "/tmp/wd" },
    maxTurns: null,
    ...over,
  } as unknown as RenderSessionRequest;
}

const codexReturning = (turn: unknown, onStart?: (o: unknown) => void): CodexCfg =>
  ({ startThread(opts: unknown) { onStart?.(opts); return { run: async () => turn }; } }) as unknown as CodexCfg;

const sig = (s: unknown) => s as Record<string, unknown> | undefined;

test("codex backend: maps a done signal + usage (output+reasoning summed; cached → cached_tokens)", async () => {
  const turn = {
    finalResponse: '{"status":"done","summary":"ok"}',
    usage: { input_tokens: 100, cached_input_tokens: 40, output_tokens: 20, reasoning_output_tokens: 5 },
  };
  const out = await createCodexRenderBackend({ codex: codexReturning(turn) }).runSession(fakeReq());
  assert.equal(sig(out.signal)?.["status"], "done");
  assert.equal(sig(out.signal)?.["summary"], "ok");
  assert.equal(out.usage.inputTokens, 100);
  assert.equal(out.usage.outputTokens, 25);
  assert.equal(out.usage.totalTokens, 125);
  assert.deepEqual(out.usage.inputTokensDetails, [{ cached_tokens: 40 }]);
});

test("codex backend: threads workingDirectory + sandbox + model into startThread", async () => {
  let opts: Record<string, unknown> | undefined;
  const backend = createCodexRenderBackend({
    sandboxMode: "read-only",
    model: "gpt-5.4-codex",
    codex: codexReturning({ finalResponse: "{}", usage: null }, (o) => { opts = o as Record<string, unknown>; }),
  });
  await backend.runSession(fakeReq({ context: { node: "n", store: {}, workingDir: "/tmp/abc" } }));
  assert.equal(opts?.["sandboxMode"], "read-only");
  assert.equal(opts?.["workingDirectory"], "/tmp/abc");
  assert.equal(opts?.["model"], "gpt-5.4-codex"); // from config, NOT req.model
  assert.equal(opts?.["skipGitRepoCheck"], true);
});

test("codex backend: a thrown turn → failed signal, zero usage (prior truth stands)", async () => {
  const codex = { startThread: () => ({ run: async () => { throw new Error("boom"); } }) } as unknown as CodexCfg;
  const out = await createCodexRenderBackend({ codex }).runSession(fakeReq());
  assert.equal(sig(out.signal)?.["status"], "failed");
  assert.equal(out.usage.totalTokens, 0);
});

test("codex backend: unparseable final message → undefined signal (harness treats as failed)", async () => {
  const out = await createCodexRenderBackend({ codex: codexReturning({ finalResponse: "no json here", usage: null }) }).runSession(fakeReq());
  assert.equal(out.signal, undefined);
});

test("parseSignal: extracts fenced + bare JSON, passes failed through, rejects junk", () => {
  assert.equal(sig(parseSignal('```json\n{"status":"failed","reason":"x"}\n```'))?.["status"], "failed");
  assert.equal(sig(parseSignal('prose before {"status":"done"} trailing'))?.["status"], "done");
  assert.equal(parseSignal("nope"), undefined);
});

const compileReq = (over: Record<string, unknown> = {}): CompileSessionRequest =>
  ({ step: "forme", instructions: "I", input: "contracts", outputType: undefined, model: "gpt-5.4-codex", modelSettings: {}, maxTurns: 100, ...over }) as unknown as CompileSessionRequest;

test("codex compile backend: parses a JSON artifact + maps usage; read-only sandbox", async () => {
  let opts: Record<string, unknown> | undefined;
  const codex = codexReturning({ finalResponse: '{"matches":[{"a":1}]}', usage: { input_tokens: 50, output_tokens: 10 } }, (o) => { opts = o as Record<string, unknown>; });
  const out = await createCodexCompileBackend({ codex }).runSession(compileReq());
  assert.deepEqual(out.output, { matches: [{ a: 1 }] });
  assert.equal(out.usage.inputTokens, 50);
  assert.equal(out.usage.outputTokens, 10);
  assert.equal(opts?.["sandboxMode"], "read-only");
});

test("parseJsonArtifact: objects, arrays, fenced, junk", () => {
  assert.deepEqual(parseJsonArtifact('```json\n{"x":1}\n```'), { x: 1 });
  assert.deepEqual(parseJsonArtifact("prefix [1,2,3] suffix"), [1, 2, 3]);
  assert.equal(parseJsonArtifact("nope"), undefined);
});
