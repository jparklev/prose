// Reactor DevTools — the Observable reactive viewer.
//
// The view is a reactive Observable notebook projecting Reactor's receipt ledger.
// Two source cells are GENERATORS the runtime drives natively (lean into the
// runtime — no manual variable.define redefine-in-place):
//   • `snapshot` = Generators.observe over the SSE stream: the /api/state baseline,
//     then each receipt.appended / state.reset pushed as a new value.
//   • `head`     = Generators.input over a `viewof` transport (the scrubber): the
//     scrub index, advanced by the slider / play / keyboard / live-edge follow.
// Every downstream cell — the dependency DAG, the cost chart, the surprise tray,
// the Inspector dataflow tree, the Inputs.table ledger — reactively depends on
// those two generators (+ the theme / dag-mode / rate / ack control vars), so a
// pushed frame or a scrub recomputes exactly what changed.
//
// Generators.observe / .input are faithful local ports in obs.mjs (the runtime
// ships only the scheduler, not the stdlib Library). Bundled by
// scripts/build-viewer.mjs (esbuild, vendored, offline) to
// src/public/viewer.bundle.js and loaded by /viewer.html.

import { Runtime } from "@observablehq/runtime";
import { Inspector } from "@observablehq/inspector";
import * as Plot from "@observablehq/plot";
import * as Inputs from "@observablehq/inputs";
import { observe, input as inputGen } from "./obs.mjs";

const SVG = "http://www.w3.org/2000/svg";
const GEOM = { nodeW: 158, nodeH: 48, colGap: 66, rowGap: 16, padX: 16, padY: 16 };

const ICONS = {
  graph: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="6" cy="6" r="2.4"/><circle cx="6" cy="18" r="2.4"/><circle cx="18" cy="12" r="2.4"/><path d="M8.4 6.7 15.6 11M8.4 17.3 15.6 13"/></svg>',
  cost: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M4 20V10M10 20V4M16 20v-7M22 20H2"/></svg>',
  bell: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M18 8a6 6 0 1 0-12 0c0 7-3 9-3 9h18s-3-2-3-9M13.7 21a2 2 0 0 1-3.4 0"/></svg>',
  braces: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M7 4a2 2 0 0 0-2 2v3a2 2 0 0 1-2 2 2 2 0 0 1 2 2v3a2 2 0 0 0 2 2M17 4a2 2 0 0 1 2 2v3a2 2 0 0 0 2 2 2 2 0 0 0-2 2v3a2 2 0 0 1-2 2"/></svg>',
  sun: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><circle cx="12" cy="12" r="4"/><path d="M12 2v2M12 20v2M4.9 4.9l1.4 1.4M17.7 17.7l1.4 1.4M2 12h2M20 12h2M4.9 19.1l1.4-1.4M17.7 6.3l1.4-1.4"/></svg>',
  moon: '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor"><path d="M21 12.8A9 9 0 1 1 11.2 3a7 7 0 0 0 9.8 9.8Z"/></svg>',
};

function el(tag, props = {}, ...kids) {
  const n = document.createElement(tag);
  for (const [k, v] of Object.entries(props)) {
    if (k === "class") n.className = v;
    else if (k === "html") n.innerHTML = v;
    else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
    else n.setAttribute(k, v);
  }
  for (const c of kids.flat()) if (c != null) n.append(c instanceof Node ? c : document.createTextNode(String(c)));
  return n;
}
function svg(tag, attrs = {}, ...kids) {
  const n = document.createElementNS(SVG, tag);
  for (const [k, v] of Object.entries(attrs)) n.setAttribute(k, String(v));
  for (const c of kids.flat()) if (c != null) n.append(c);
  return n;
}
function into(sel) {
  const node = typeof sel === "string" ? document.querySelector(sel) : sel;
  return {
    pending() { if (node) node.dataset.pending = "1"; },
    fulfilled(v) {
      if (!node) return;
      delete node.dataset.pending;
      node.replaceChildren(v instanceof Node ? v : document.createTextNode(v == null ? "" : String(v)));
    },
    rejected(e) {
      if (!node) return;
      node.replaceChildren(el("pre", { class: "cell-error" }, String((e && e.stack) || e)));
    },
  };
}

// ---- layout: longest-path layering (ported from the baseline viewer) -------
function buildLayout(snapshot) {
  const ids = new Set();
  const entry = new Set(snapshot.entryPoints);
  for (const n of snapshot.nodes) ids.add(n.id);
  for (const e of snapshot.edges) { ids.add(e.producer); ids.add(e.subscriber); }
  const producers = new Map();
  for (const id of ids) producers.set(id, []);
  for (const e of snapshot.edges) producers.get(e.subscriber).push(e.producer);

  const layerOf = new Map(); const visiting = new Set();
  const layer = (id) => {
    if (layerOf.has(id)) return layerOf.get(id);
    if (visiting.has(id)) return 0;
    visiting.add(id);
    let l = 0;
    for (const p of producers.get(id) || []) l = Math.max(l, layer(p) + 1);
    visiting.delete(id); layerOf.set(id, l); return l;
  };
  for (const id of ids) layer(id);

  const layers = [];
  for (const id of ids) { const l = layerOf.get(id); (layers[l] || (layers[l] = [])).push(id); }
  const order = new Map();
  const bary = (id) => {
    const ps = producers.get(id) || [];
    if (!ps.length) return 0;
    let s = 0; for (const p of ps) s += order.get(p) ?? 0; return s / ps.length;
  };
  layers.forEach((bucket, li) => {
    bucket.sort(li === 0 ? undefined : (a, b) => bary(a) - bary(b) || (a < b ? -1 : 1));
    bucket.forEach((id, i) => order.set(id, i));
  });

  const { nodeW, nodeH, colGap, rowGap, padX, padY } = GEOM;
  const colStride = nodeW + colGap, rowStride = nodeH + rowGap;
  const tallest = layers.reduce((m, b) => Math.max(m, b ? b.length : 0), 0);
  const totalH = tallest * rowStride - rowGap;
  const nodes = new Map();
  layers.forEach((bucket, li) => {
    if (!bucket) return;
    const colH = bucket.length * rowStride - rowGap;
    const yOff = padY + (totalH - colH) / 2;
    bucket.forEach((id, ri) => nodes.set(id, {
      id, x: padX + li * colStride, y: yOff + ri * rowStride, w: nodeW, h: nodeH, isEntry: entry.has(id),
    }));
  });
  const width = padX * 2 + (layers.length - 1) * colStride + nodeW;
  const height = Math.max(padY * 2 + totalH, 200);
  const edges = snapshot.edges.map((e) => ({ ...e, key: `${e.producer}→${e.subscriber}::${e.facet}`, a: nodes.get(e.producer), b: nodes.get(e.subscriber) }));
  return { nodes, edges, width, height };
}

function curve(a, b) {
  const x1 = a.x + a.w, y1 = a.y + a.h / 2, x2 = b.x, y2 = b.y + b.h / 2, mx = (x1 + x2) / 2;
  return `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`;
}
const shortName = (id, labels) => labels[id] ?? (id.includes(".") ? id.slice(id.indexOf(".") + 1) : id);
const kindOf = (id, labels) => (labels[id] ? "" : id.includes(".") ? id.slice(0, id.indexOf(".")) : "");

// node disposition + propagation state at the scrub head
function stateUpTo(frames, head) {
  const statusById = new Map();
  for (let i = 0; i <= head && i < frames.length; i++) statusById.set(frames[i].node, frames[i].status);
  const cur = head >= 0 && head < frames.length ? frames[head] : null;
  const lit = new Set((cur?.edgesToLight ?? []).map((e) => `${e.producer}→${e.subscriber}::${e.facet}`));
  return { statusById, activeNode: cur?.node ?? null, lit };
}

function renderDag(snapshot, layout, head, mode) {
  const labels = snapshot.labels || {};
  const heat = mode !== "path"; // "heat" = last-disposition per node; "path" = active node + lit edges only
  const { statusById, activeNode, lit } = stateUpTo(snapshot.frames, head);
  const edgeG = svg("g", { class: "edges" });
  for (const e of layout.edges) {
    if (!e.a || !e.b) continue;
    edgeG.append(svg("path", { class: "edge" + (lit.has(e.key) ? " lit" : ""), d: curve(e.a, e.b) }));
  }
  const nodeG = svg("g", { class: "nodes" });
  for (const n of layout.nodes.values()) {
    const last = statusById.get(n.id) || "idle";
    // heat-map: colour every node by its last disposition. active-path: settled
    // nodes go neutral; only the active node (its real status) + failures show.
    let status = heat ? last : (last === "failed" ? "failed" : "neutral");
    if (n.id === activeNode) status = last;
    const cls = ["node", `s-${status}`];
    if (n.isEntry) cls.push("entry");
    if (n.id === activeNode) cls.push("active", "flash");
    const g = svg("g", { class: cls.join(" "), transform: `translate(${n.x} ${n.y})` });
    g.append(svg("rect", { class: "nbox", x: 0, y: 0, width: n.w, height: n.h, rx: 9 }));
    const kind = kindOf(n.id, labels);
    if (kind) g.append(svg("text", { class: "nkind", x: 13, y: 18 }, document.createTextNode(kind)));
    g.append(svg("text", { class: "nlabel", x: 13, y: kind ? n.h - 16 : n.h / 2 + 4 }, document.createTextNode(shortName(n.id, labels))));
    nodeG.append(g);
  }
  const s = svg("svg", { class: "dag", viewBox: `0 0 ${layout.width} ${layout.height}`, preserveAspectRatio: "xMidYMid meet" }, edgeG, nodeG);
  return s;
}

const fmt = (n) => n.toLocaleString();

function costChart(frames, theme, total) {
  const dark = theme === "dark";
  const ink = dark ? "#a1a1aa" : "#71717a";
  const C = { rendered: dark ? "#fb923c" : "#ea580c", skipped: dark ? "#52525b" : "#a1a1aa", failed: dark ? "#f87171" : "#dc2626" };
  return Plot.plot({
    width: 1000, height: 188, marginLeft: 52, marginBottom: 26,
    style: { background: "transparent", color: ink, fontSize: "11px" },
    // Fixed domain over the whole ledger — bars fill in left→right as `head`
    // advances; the axis never rescales / squeezes prior receipts.
    x: { label: "receipt frame →", domain: [-0.5, total - 0.5], ticks: 10 },
    y: { label: "fresh tokens", grid: true, tickFormat: "~s" },
    marks: [
      Plot.ruleY([0], { stroke: dark ? "#27272a" : "#e4e4e7" }),
      Plot.rectY(frames, { x: "index", interval: 1, y: (d) => d.cost.fresh, fill: (d) => C[d.status] ?? "#888", inset: 0.5,
        title: (d) => `${d.node}\n${d.status} · ${d.cost.fresh} fresh · ${d.cost.surpriseCause}` }),
    ],
  });
}

// API-equivalent $ (ccusage / codex-usage spirit): even on a flat-rate subscription
// backend you can price token counts at API rates to get a sense of cost. `fresh`
// tokens are what was actually spent; `reused` is what memoization saved.
const usd = (tokens, ratePerMtok) => {
  const d = (tokens / 1e6) * ratePerMtok;
  return "$" + d.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: d < 1 ? 4 : 2 });
};

function dispo(frames, labels, rate) {
  const c = { rendered: 0, skipped: 0, failed: 0 }; let fresh = 0, reused = 0;
  for (const f of frames) { c[f.status] = (c[f.status] ?? 0) + 1; fresh += f.cost.fresh; reused += f.cost.reused; }
  const pct = fresh + reused > 0 ? Math.round((100 * reused) / (fresh + reused)) : 0;
  const chip = (label, n, key) => el("span", { class: "chip" }, el("i", { class: `sw-${key}`, style: `background:var(--${key === "rendered" ? "accent" : key === "failed" ? "fail" : "skip"})` }), `${label} ${n}`);
  return el("div", { class: "dispo" },
    chip("rendered", c.rendered, "rendered"), chip("skipped", c.skipped, "skipped"), chip("failed", c.failed, "failed"),
    el("span", { class: "dispo-cost" },
      `fresh ${fmt(fresh)} · reused ${fmt(reused)} · ${pct}% reuse · `,
      el("b", {}, `≈ ${usd(fresh, rate)}`), ` spent · `,
      el("span", { class: "saved" }, `${usd(reused, rate)} saved`),
      ` api-equiv`));
}

// ---- the ledger as a sortable Inputs.table (Observable idiom) --------------
// One row per committed receipt frame ≤ head, columns sorted/​reversed so the
// newest disposition is on top. Click a column header to re-sort — the receipt
// ledger as a first-class Observable input.
function ledgerTable(frames, labels) {
  const rows = frames.map((f) => ({
    "#": f.index,
    node: shortName(f.node, labels),
    status: f.status,
    fresh: f.cost.fresh,
    reused: f.cost.reused,
    moved: (f.movedFacets || []).join(", "),
    cause: f.cost.surpriseCause ?? "",
  }));
  return Inputs.table(rows, {
    columns: ["#", "node", "status", "fresh", "reused", "moved", "cause"],
    header: { "#": "#", fresh: "fresh tok", reused: "reused tok" },
    sort: "#",
    reverse: true,
    rows: 13,
    width: { "#": 40, status: 86, fresh: 78, reused: 80 },
    format: {
      fresh: (v) => v.toLocaleString(),
      reused: (v) => v.toLocaleString(),
      status: (v) => Object.assign(document.createElement("span"), { className: `tbl-status s-${v}`, textContent: v }),
    },
  });
}

// ---- surprise projection: pure functions over committed facts -------------
// Surprises are DERIVED from the committed receipt frames (+ moved facets). They
// never gate wake/commit — they are sideband attention. Per Gemini's review:
// status transitions active → historical (cleared by a later frame) →
// acknowledged (human), so transient/flaky failures aren't hidden.
const VERDICT_RE = /verdict|risk|status|gate|level|decision|alert/i;
const median = (xs) => { if (!xs.length) return 0; const s = [...xs].sort((a, b) => a - b); const m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };

function projectSurprises(frames, chainVerify, invariants) {
  const events = [];
  const hist = new Map(); // node -> prior nonzero fresh costs (chronological)
  for (const f of frames) {
    if (f.status === "failed") {
      events.push({ id: `failed:${f.node}:${f.index}`, cause: "receipt-failed", severity: "block", node: f.node, frameIndex: f.index, reason: "render failed its postconditions — prior truth stands" });
    } else if (f.status === "rendered") {
      const flips = (f.movedFacets || []).filter((x) => VERDICT_RE.test(x));
      if (flips.length) events.push({ id: `flip:${f.node}:${f.index}`, cause: "verdict-facet-flip", severity: "warn", node: f.node, frameIndex: f.index, reason: `verdict facet moved — ${flips.join(", ")}` });
      const h = hist.get(f.node) || [];
      const fresh = f.cost.fresh;
      if (fresh > 0) {
        // cost-spike (Gemini): need >=5 prior nonzero renders for this node AND an
        // absolute floor, before flagging > median(last 8) x k. Kills cold-boot noise.
        if (h.length >= 5 && fresh >= 200) {
          const med = median(h.slice(-8));
          if (med > 0 && fresh > med * 3) events.push({ id: `spike:${f.node}:${f.index}`, cause: "cost-spike", severity: "warn", node: f.node, frameIndex: f.index, reason: `fresh ${fresh.toLocaleString()} ≫ ${Math.round(med).toLocaleString()} median (×${(fresh / med).toFixed(1)})` });
        }
        h.push(fresh); hist.set(f.node, h);
      }
    }
  }
  for (const ev of events) {
    // historical iff a later committed frame supersedes it (for a failure that
    // means a later *rendered* frame = recovery; otherwise any later frame).
    const cleared = frames.some((f) => f.node === ev.node && f.index > ev.frameIndex && (ev.cause === "receipt-failed" ? f.status === "rendered" : true));
    ev.status = cleared ? "historical" : "active";
  }
  // chain-verify-break: a node whose committed receipt chain fails to verify
  // (a content hash ≠ its payload). Server-computed over the RAW on-disk
  // receipts; does not auto-resolve. Surfaced once the node is in scrubbed range.
  const lastIdx = new Map();
  for (const f of frames) lastIdx.set(f.node, f.index);
  if (chainVerify) {
    for (const [node, ok] of Object.entries(chainVerify)) {
      if (!ok && lastIdx.has(node)) {
        events.push({ id: `chain:${node}`, cause: "chain-verify-break", severity: "block", node, frameIndex: lastIdx.get(node), reason: "receipt chain failed to verify — a content hash does not match its payload", status: "active" });
      }
    }
  }
  // invariant-failed: a declared truth invariant (the observability.json sidecar)
  // the node's LATEST committed truth violates. Server-computed against the truth;
  // SIDEBAND — never gated a wake. Surfaced once the head reaches the node's last
  // frame (where the offending truth was committed), like chain-verify-break.
  if (invariants) {
    for (const b of invariants) {
      const within = lastIdx.has(b.node) && b.frameIndex <= lastIdx.get(b.node);
      if (within) events.push({ id: `inv:${b.id}`, cause: "invariant-failed", severity: b.severity || "warn", node: b.node, frameIndex: b.frameIndex, reason: `${b.label} — ${b.reason}`, status: "active" });
    }
  }
  return events;
}

function renderTray(frames, labels, acked, jump, ack, chainVerify, invariants) {
  const events = projectSurprises(frames, chainVerify, invariants);
  for (const e of events) if (acked.has(e.id)) e.status = "acknowledged";
  const SEV = { block: 0, warn: 1, info: 2 };
  const active = events.filter((e) => e.status === "active").sort((a, b) => SEV[a.severity] - SEV[b.severity] || b.frameIndex - a.frameIndex);
  const past = events.filter((e) => e.status !== "active").sort((a, b) => b.frameIndex - a.frameIndex);

  const card = (s) => el("li", { class: `surprise sev-${s.severity} st-${s.status}` },
    el("span", { class: "sev" }, s.severity),
    el("span", { class: "surprise-node", onclick: () => jump(s.frameIndex) }, shortName(s.node, labels)),
    el("span", { class: "surprise-cause" }, s.cause),
    el("span", { class: "surprise-frame", onclick: () => jump(s.frameIndex) }, `frame ${s.frameIndex}`),
    el("button", { class: "ack", title: s.status === "acknowledged" ? "un-acknowledge" : "acknowledge", onclick: () => ack(s.id) }, s.status === "acknowledged" ? "✓" : "○"),
    el("div", { class: "surprise-reason" }, s.reason));

  const wrap = el("div", { class: "tray-wrap" });
  if (!active.length) wrap.append(el("div", { class: "tray-empty" }, past.length ? "no active surprises — the ledger is calm" : "no surprises — the ledger is calm"));
  else wrap.append(el("ul", { class: "tray" }, active.map(card)));
  if (past.length) {
    wrap.append(el("div", { class: "tray-sep" }, `${past.length} resolved / acknowledged`));
    wrap.append(el("ul", { class: "tray past" }, past.map(card)));
  }
  return wrap;
}

// ---- the `viewof` transport: a scrubber over the existing transport DOM -------
// Wraps the styled #t-start/#t-play/#t-end/#seek/#readout controls into ONE
// element whose `.value` is the scrub head (an int in [-1, max]) and which
// dispatches "input" on change — so `Generators.input(scrubber.host)` makes `head`
// a reactive cell. Owns play/pause, keyboard (space / ←→), and live growth
// (`grow` bumps the range max and follows the edge as frames stream in).
function makeScrubber(initialFrames, labels) {
  const host = document.querySelector(".transport");
  const seek = document.querySelector("#seek");
  const readout = document.querySelector("#readout");
  const playBtn = document.querySelector("#t-play");
  let frames = initialFrames;
  let max = frames.length - 1;
  let head = max;
  let timer = null;

  // `host.value` IS the head — what Generators.input reads on each "input".
  Object.defineProperty(host, "value", { get: () => head, configurable: true });
  const emit = () => host.dispatchEvent(new Event("input", { bubbles: true }));
  const sync = () => {
    seek.min = "-1"; seek.max = String(max); seek.value = String(head);
    const f = head >= 0 ? frames[head] : null;
    readout.innerHTML = f
      ? `frame <b>${head}</b>/${max} · ${shortName(f.node, labels)} · ${f.status}`
      : `frame —/${max}`;
  };
  const set = (i, silent = false) => { head = Math.max(-1, Math.min(max, i)); sync(); if (!silent) emit(); };
  const stop = () => { if (timer) { clearInterval(timer); timer = null; playBtn.textContent = "▶"; } };
  const play = () => {
    if (timer) return stop();
    if (head >= max) set(-1);
    playBtn.textContent = "⏸";
    timer = setInterval(() => { if (head >= max) return stop(); set(head + 1); }, 320);
  };

  playBtn.onclick = play;
  document.querySelector("#t-start").onclick = () => { stop(); set(-1); };
  document.querySelector("#t-end").onclick = () => { stop(); set(max); };
  seek.oninput = () => { stop(); set(parseInt(seek.value, 10)); };
  window.addEventListener("keydown", (e) => {
    if (e.target && /^(input|textarea|select)$/i.test(e.target.tagName)) return;
    if (e.key === " ") { e.preventDefault(); play(); }
    else if (e.key === "ArrowRight") { stop(); set(head + 1); }
    else if (e.key === "ArrowLeft") { stop(); set(head - 1); }
  });

  sync(); // reflect the initial head (the edge) in the slider + readout on boot

  return {
    host,
    setLabels(l) { labels = l; },
    // jump to a frame (the surprise tray's click-through) — stop playback, seek.
    goto(i) { stop(); set(i); },
    // a frame streamed in: extend the range; follow the edge iff we were parked there.
    grow(next) { const wasEdge = head >= max; frames = next; max = frames.length - 1; if (wasEdge) set(max); else sync(); },
    // a full state.reset: re-baseline and jump to the new edge.
    reset(next) { frames = next; max = frames.length - 1; stop(); set(max); },
  };
}

async function boot() {
  let snapshot = await (await fetch("/api/state")).json();
  let labels = snapshot.labels || {};
  let layout = buildLayout(snapshot);
  const N = snapshot.frames.length;

  // header + nav
  document.querySelector("#nb-ex").textContent = (snapshot.stateDir || "").split("/").filter(Boolean).slice(-2, -1)[0] || "reactor";
  document.querySelector("#nb-counts").textContent = `${N} receipts · ${snapshot.nodes.length} nodes · ${snapshot.edges.length} edges`;
  const nav = document.querySelector("#nav");
  for (const [id, label, icon] of [["sec-graph", "Dependency graph", "graph"], ["sec-cost", "Cost", "cost"], ["sec-surprises", "Surprises", "bell"], ["sec-dataflow", "Dataflow", "braces"]]) {
    nav.append(el("a", { class: "navitem", "data-sec": id, onclick: () => document.querySelector("#" + id).scrollIntoView({ behavior: "smooth", block: "start" }) },
      el("span", { class: "ico", html: ICONS[icon] }), el("span", {}, label)));
  }
  const navItems = [...nav.querySelectorAll(".navitem")];
  // active nav on scroll
  const io = new IntersectionObserver((es) => {
    for (const e of es) if (e.isIntersecting) navItems.forEach((a) => a.classList.toggle("active", a.dataset.sec === e.target.id));
  }, { rootMargin: "-20% 0px -70% 0px" });
  ["sec-graph", "sec-cost", "sec-surprises", "sec-dataflow"].forEach((id) => io.observe(document.querySelector("#" + id)));

  // theme
  const themeBtn = document.querySelector("#theme");
  const applyTheme = (t) => {
    document.documentElement.dataset.theme = t;
    themeBtn.innerHTML = t === "dark" ? ICONS.sun : ICONS.moon;
    localStorage.setItem("reactor-devtools-theme", t);
  };

  // --- runtime: the two source generators + the discrete control vars ---------
  const runtime = new Runtime();
  const main = runtime.module();
  const footState = document.querySelector("#foot-state");

  // `head` = Generators.input over the `viewof` transport (scrubber). A reactive
  // cell: scrub / play / keyboard / live-edge-follow all flow through the host's
  // "input" event.
  const transport = makeScrubber(snapshot.frames, labels);
  const goto = (i) => transport.goto(i);
  window.__reactorSetHead = goto; // Chrome-driving hook (e2e walkthroughs)
  main.variable().define("head", [], () => inputGen(transport.host));

  // `snapshot` = Generators.observe over the SSE stream. The initial /api/state is
  // the first push; every receipt.appended extends it, every state.reset replaces
  // it. The generator owns the live wire — the transport follows the edge and the
  // header/footer update as values flow, then `change(snap)` recomputes the graph.
  main.variable().define("snapshot", [], () =>
    observe((change) => {
      change(snapshot);
      let es;
      try {
        const lastHash = snapshot.frames[snapshot.frames.length - 1]?.contentHash || "";
        es = new EventSource(`/events?after=${snapshot.frames.length - 1}&hash=${encodeURIComponent(lastHash)}`);
      } catch { return undefined; } // no EventSource — static replay over the baseline
      const counts = () => { document.querySelector("#nb-counts").textContent = `${snapshot.frames.length} receipts · ${snapshot.nodes.length} nodes · ${snapshot.edges.length} edges`; };
      es.addEventListener("message", (e) => {
        let msg; try { msg = JSON.parse(e.data); } catch { return; }
        if (msg.type === "receipt.appended") {
          // merge the server's freshly-computed observer-side properties so the
          // live tray (chain-verify / invariant breaches) tracks the new truth.
          snapshot = { ...snapshot, frames: [...snapshot.frames, msg.frame], chainVerify: msg.chainVerify ?? snapshot.chainVerify, invariants: msg.invariants ?? snapshot.invariants };
          transport.grow(snapshot.frames); // extend the range; follow the edge
          counts();
          footState.textContent = "● live"; footState.style.color = "var(--accent)";
          change(snapshot);
        } else if (msg.type === "state.reset") {
          snapshot = msg.snapshot;
          labels = snapshot.labels || {}; // refresh label-derived rendering
          layout = buildLayout(snapshot);
          transport.setLabels(labels);
          transport.reset(snapshot.frames);
          counts();
          change(snapshot);
        }
      });
      es.onerror = () => { footState.textContent = "replay"; footState.style.color = ""; };
      return () => es && es.close();
    }));

  // Discrete UI controls stay redefine-in-place (they are toggles, not streams):
  // theme, the DAG mode, the api-equivalent rate, and the surprise-ack counter.
  let theme = localStorage.getItem("reactor-devtools-theme") || "light";
  const themeVar = main.variable(); themeVar.define("theme", [], () => theme);
  const setTheme = (t) => { theme = t; applyTheme(t); themeVar.define("theme", [], () => t); };
  const acked = new Set(); let ackN = 0;
  const ackVar = main.variable(); ackVar.define("ack", [], () => ackN);
  const toggleAck = (id) => { if (acked.has(id)) acked.delete(id); else acked.add(id); ackVar.define("ack", [], () => ++ackN); };
  let dagMode = localStorage.getItem("reactor-dag-mode") || "heat";
  const dagModeVar = main.variable(); dagModeVar.define("dagMode", [], () => dagMode);
  const setDagMode = (m) => { dagMode = m; localStorage.setItem("reactor-dag-mode", m); dagModeVar.define("dagMode", [], () => m); const b = document.querySelector("#dag-toggle"); if (b) b.textContent = m === "heat" ? "heat-map" : "active path"; };
  let rate = Number(localStorage.getItem("reactor-rate")) || 5; // $/Mtok, API-equivalent
  const rateVar = main.variable(); rateVar.define("rate", [], () => rate);
  const setRate = (v) => { rate = v; localStorage.setItem("reactor-rate", String(v)); rateVar.define("rate", [], () => v); };

  // --- derived cells: everything reactively recomputes off snapshot + head -----
  main.variable().define("framesUpTo", ["snapshot", "head"], (s, h) => s.frames.slice(0, h + 1));
  main.variable(into("#cell-dag")).define("dag", ["snapshot", "head", "dagMode"], (s, h, m) => renderDag(s, layout, h, m));
  main.variable(into("#cell-cost")).define("costChart", ["framesUpTo", "theme", "snapshot"], (f, t, s) => costChart(f, t, s.frames.length));
  main.variable(into("#cell-dispo")).define("dispoCell", ["framesUpTo", "rate"], (f, r) => dispo(f, labels, r));
  main.variable(into("#cell-tray")).define("trayCell", ["framesUpTo", "ack", "snapshot"], (f, a, s) => renderTray(f, labels, acked, goto, toggleAck, s.chainVerify, s.invariants));
  // Dataflow — lean into the runtime: the live derived state as the iconic
  // expandable Inspector tree, and the ledger as a sortable Inputs.table.
  main.variable(new Inspector(document.querySelector("#cell-inspector"))).define("dataflow", ["framesUpTo", "snapshot"], (framesUpTo, s) => ({
    "ledger · frames ≤ head": framesUpTo,
    costRollup: s.costRollup,
    nodes: s.nodes.map((n) => n.id),
    edges: s.edges,
    chainVerify: s.chainVerify,
    invariants: s.invariants,
  }));
  main.variable(into("#cell-ledger-table")).define("ledgerTable", ["framesUpTo"], (f) => ledgerTable(f, labels));

  // settled-node toggle (#4a): heat-map (last disposition) vs active-path-only
  const graphHead = document.querySelector("#sec-graph h3");
  if (graphHead) graphHead.append(el("button", { id: "dag-toggle", class: "cell-toggle", onclick: () => setDagMode(dagMode === "heat" ? "path" : "heat") }, dagMode === "heat" ? "heat-map" : "active path"));

  // API-equivalent price control (ccusage spirit): $/Mtok applied to fresh/reused.
  const costHead = document.querySelector("#sec-cost h3");
  if (costHead) {
    const inp = el("input", { id: "rate-input", class: "rate-input", type: "number", min: "0", step: "0.5", value: String(rate), title: "API-equivalent price, $/Mtok" });
    inp.addEventListener("input", () => setRate(Number(inp.value) || 0));
    costHead.append(el("span", { class: "rate-ctl" }, "$", inp, "/Mtok api-equiv"));
  }

  themeBtn.onclick = () => setTheme(theme === "dark" ? "light" : "dark");
  setTheme(theme);
}

boot().catch((err) => document.body.append(el("pre", { class: "cell-error" }, String(err && err.stack ? err.stack : err))));
