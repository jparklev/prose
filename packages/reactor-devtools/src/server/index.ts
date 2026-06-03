// The SERVER — a tiny Node `http` server (zero runtime dep beyond the SDK).
//
// The viewer is a READ-ONLY projection of the receipt ledger. Replay (static
// state-dir) and live (`reactor serve` writing the same dir) are the same
// projection: `/events` emits append-only frame deltas. The ledger is a single
// JSON array written atomically (temp + rename), so a reader never sees a torn
// file — on any change we re-read the whole file, rebuild the snapshot, and diff
// against the last one. Contiguous extension → `receipt.appended` deltas;
// otherwise (truncation, prefix or metadata change) → `state.reset`. Strictly
// observer-side: we only READ committed receipts; nothing here gates a render.
//
// Hardening (Codex review): clients carry their baseline frame HASH so a reset
// between /api/state and /events can't graft new history onto an old snapshot;
// the diff also resets on metadata (nodes/edges/labels) change, not just frames;
// SSE writes are back-pressure / disconnect aware. Per-node chain-verify is
// surfaced in the snapshot so the viewer can flag a tampered ledger.

import { createServer, type Server, type IncomingMessage, type ServerResponse } from "node:http";
import { readFileSync, existsSync, watch, type FSWatcher } from "node:fs";
import { join, normalize } from "node:path";

import {
  openStateDir,
  buildSnapshot,
  readNodeWorldModel,
  verifyNodeChainRaw,
  type OpenedStateDir,
  type ReplaySnapshot,
} from "../data";

export interface DevToolsServerOptions {
  readonly stateDir: string;
  readonly port?: number;
  readonly host?: string;
}

export interface DevToolsServer {
  readonly server: Server;
  readonly url: string;
  readonly snapshot: ReplaySnapshot;
  close(): Promise<void>;
}

const DEFAULT_PORT = 4555;
const DEFAULT_HOST = "127.0.0.1";

function publicDir(): string {
  const built = join(__dirname, "..", "public");
  if (existsSync(join(built, "index.html"))) return built;
  return join(__dirname, "..", "..", "src", "public");
}

const CONTENT_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".map": "application/json; charset=utf-8",
};

function contentTypeFor(path: string): string {
  const dot = path.lastIndexOf(".");
  const ext = dot >= 0 ? path.slice(dot) : "";
  return CONTENT_TYPES[ext] ?? "application/octet-stream";
}

type Frame = ReplaySnapshot["frames"][number];
// The snapshot we serve, augmented with per-node chain-verify (committed-fact
// tamper evidence the viewer turns into a `chain-verify-break` surprise).
type LiveSnapshot = ReplaySnapshot & { chainVerify: Record<string, boolean> };
type DevtoolsEvent =
  | { type: "receipt.appended"; frame: Frame; chainVerify: Record<string, boolean> }
  | { type: "state.reset"; snapshot: LiveSnapshot }
  | { type: "hello"; frames: number };

/** Stable key over the NON-frame projection, so a metadata change forces reset. */
function metaKey(s: ReplaySnapshot): string {
  return JSON.stringify({ nodes: s.nodes, edges: s.edges, entryPoints: s.entryPoints, labels: s.labels, hasTopology: s.hasTopology, acyclic: s.acyclic });
}

function computeChainVerify(opened: OpenedStateDir, frames: readonly Frame[]): Record<string, boolean> {
  const out: Record<string, boolean> = {};
  const nodes = new Set(frames.map((f) => f.node));
  for (const node of nodes) {
    try { out[node] = verifyNodeChainRaw(opened, node).ok; } catch { out[node] = true; }
  }
  return out;
}

class LiveProjector {
  readonly stateDir: string;
  opened: OpenedStateDir;
  snapshot: LiveSnapshot;
  private meta: string;
  private readonly clients = new Set<ServerResponse>();
  private watcher: FSWatcher | null = null;
  private timer: NodeJS.Timeout | null = null;
  private keepalive: NodeJS.Timeout | null = null;

  constructor(stateDir: string) {
    this.stateDir = stateDir;
    this.opened = openStateDir(stateDir);
    this.snapshot = this.augment(this.opened, buildSnapshot(this.opened));
    this.meta = metaKey(this.snapshot);
    this.startWatch();
    this.keepalive = setInterval(() => this.broadcastRaw(": ping\n\n"), 25000);
    this.keepalive.unref?.();
  }

  private augment(opened: OpenedStateDir, snap: ReplaySnapshot): LiveSnapshot {
    return Object.assign({}, snap, { chainVerify: computeChainVerify(opened, snap.frames) });
  }

  snapshotJson(): string {
    return JSON.stringify(this.snapshot);
  }

  private startWatch(): void {
    try {
      this.watcher = watch(this.stateDir, { persistent: false }, (_ev, fname) => {
        if (fname && !String(fname).startsWith("receipts")) return;
        this.schedule();
      });
    } catch { /* fs.watch unsupported — replay still works, just not live */ }
  }

  private schedule(): void {
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => this.onChange(), 40); // debounce coalesces batch writes
  }

  private onChange(): void {
    let next: LiveSnapshot;
    let opened: OpenedStateDir;
    try {
      opened = openStateDir(this.stateDir);
      next = this.augment(opened, buildSnapshot(opened));
    } catch {
      this.schedule(); // mid-rename / transient — retry shortly
      return;
    }
    const prev = this.snapshot.frames;
    const nf = next.frames;
    const nextMeta = metaKey(next);
    const framesContiguous = nf.length >= prev.length && prev.every((f, i) => nf[i]?.contentHash === f.contentHash);
    const metaSame = nextMeta === this.meta;
    this.opened = opened;
    this.snapshot = next;
    this.meta = nextMeta;
    if (framesContiguous && metaSame && nf.length > prev.length) {
      // Codex #2: only emit append deltas when the graph metadata is unchanged.
      for (let i = prev.length; i < nf.length; i++) {
        this.broadcast({ type: "receipt.appended", frame: nf[i]!, chainVerify: next.chainVerify });
      }
    } else if (!framesContiguous || !metaSame) {
      this.broadcast({ type: "state.reset", snapshot: next });
    }
  }

  private broadcast(ev: DevtoolsEvent): void {
    this.broadcastRaw(`data: ${JSON.stringify(ev)}\n\n`);
  }
  private broadcastRaw(line: string): void {
    for (const res of this.clients) this.write(res, line);
  }
  // Codex #4: back-pressure / disconnect aware — never buffer onto a dead socket.
  private write(res: ServerResponse, line: string): void {
    if (res.destroyed || res.writableEnded) { this.clients.delete(res); return; }
    try {
      const ok = res.write(line);
      if (!ok) { /* slow consumer — drop it rather than buffer unboundedly */ res.end(); this.clients.delete(res); }
    } catch { this.clients.delete(res); }
  }

  addClient(res: ServerResponse, after: number | null, hash: string | null): void {
    const nf = this.snapshot.frames;
    if (after !== null && Number.isFinite(after)) {
      // Codex #1: the client's baseline is (index, hash). If the hash at `after`
      // no longer matches, the ledger was reset/replaced — send a full reset
      // instead of grafting new history onto the client's stale snapshot.
      const baselineOk = after < 0 || (hash !== null && nf[after]?.contentHash === hash);
      if (!baselineOk || after > nf.length - 1) {
        this.write(res, `data: ${JSON.stringify({ type: "state.reset", snapshot: this.snapshot })}\n\n`);
      } else {
        for (let i = after + 1; i < nf.length; i++) {
          this.write(res, `data: ${JSON.stringify({ type: "receipt.appended", frame: nf[i]!, chainVerify: this.snapshot.chainVerify })}\n\n`);
        }
      }
    } else {
      this.write(res, `data: ${JSON.stringify({ type: "hello", frames: nf.length })}\n\n`);
    }
    this.clients.add(res);
    res.on("close", () => this.clients.delete(res));
    res.on("error", () => this.clients.delete(res));
  }

  close(): void {
    if (this.timer) clearTimeout(this.timer);
    if (this.keepalive) clearInterval(this.keepalive);
    this.watcher?.close();
    for (const res of this.clients) { try { res.end(); } catch { /* ignore */ } }
    this.clients.clear();
  }
}

export async function startDevToolsServer(options: DevToolsServerOptions): Promise<DevToolsServer> {
  const port = options.port ?? DEFAULT_PORT;
  const host = options.host ?? DEFAULT_HOST;
  const projector = new LiveProjector(options.stateDir);
  const assetsDir = publicDir();
  const server = createServer((req, res) => handle(req, res, projector, assetsDir));

  await new Promise<void>((resolve, reject) => {
    const onError = (err: NodeJS.ErrnoException) => {
      if (err.code === "EADDRINUSE") reject(new Error(`Port ${port} on ${host} is already in use. Pass a different port with --port/-p (e.g. --port ${port + 1}).`));
      else reject(err);
    };
    server.once("error", onError);
    server.listen(port, host, () => { server.removeListener("error", onError); resolve(); });
  });
  const address = server.address();
  const boundPort = typeof address === "object" && address !== null ? address.port : port;
  const url = `http://${host}:${boundPort}/`;
  return {
    server,
    url,
    snapshot: projector.snapshot,
    close: () => new Promise<void>((resolve, reject) => { projector.close(); server.close((err) => (err ? reject(err) : resolve())); }),
  };
}

function sendJson(res: ServerResponse, status: number, body: unknown): void {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(body));
}

function handle(req: IncomingMessage, res: ServerResponse, projector: LiveProjector, assetsDir: string): void {
  const url = req.url ?? "/";
  const path = url.split("?")[0] ?? "/";

  if (path === "/api/state" || path === "/api/snapshot") {
    res.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    res.end(projector.snapshotJson());
    return;
  }

  if (path.startsWith("/api/node/")) {
    const node = decodeURIComponent(path.slice("/api/node/".length));
    if (node.length === 0) { sendJson(res, 400, { error: "missing node id" }); return; }
    const qs = url.includes("?") ? url.slice(url.indexOf("?") + 1) : "";
    const version = new URLSearchParams(qs).get("version");
    if (version === null || version.length === 0) { sendJson(res, 400, { error: "missing ?version= (a frame's atomicVersion)" }); return; }
    const view = readNodeWorldModel(projector.opened, node, version);
    if (view === null) { sendJson(res, 404, { error: "no world-model for node@version", node, version }); return; }
    sendJson(res, 200, view);
    return;
  }

  if (path === "/events") {
    res.writeHead(200, {
      "content-type": "text/event-stream",
      "cache-control": "no-cache",
      connection: "keep-alive",
      "x-accel-buffering": "no",
    });
    res.write(": connected\n\n");
    const qs = url.includes("?") ? url.slice(url.indexOf("?") + 1) : "";
    const params = new URLSearchParams(qs);
    const afterRaw = params.get("after");
    const after = afterRaw === null ? null : Number.parseInt(afterRaw, 10);
    projector.addClient(res, after, params.get("hash"));
    return;
  }

  const rel = path === "/" ? "index.html" : path.replace(/^\/+/, "");
  const safe = normalize(rel).replace(/^(\.\.[/\\])+/, "");
  const file = join(assetsDir, safe);
  if (!file.startsWith(assetsDir) || !existsSync(file)) {
    res.writeHead(404, { "content-type": "text/plain" });
    res.end("Not found");
    return;
  }
  res.writeHead(200, { "content-type": contentTypeFor(file) });
  res.end(readFileSync(file));
}
