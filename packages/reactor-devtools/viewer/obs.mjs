// Generators.observe / Generators.input — faithful, dependency-free ports of the
// Observable stdlib (MIT, github.com/observablehq/stdlib/blob/main/src/generators).
//
// `@observablehq/runtime` deliberately ships ONLY the dataflow scheduler, not the
// stdlib `Library`, so the two generators this viewer leans on live here. The
// runtime natively drives a generator cell: it pulls `.next()` on each animation
// frame and recomputes EVERY downstream cell with the yielded value. That is the
// reactive seam the viewer is built on — a live SSE stream (`observe`) and a
// `viewof` transport (`input`) become ordinary cells the DAG / cost / tray
// reactively depend on. No manual `variable.define` redefine-in-place.

/**
 * Turn a push source (events, a socket, an SSE stream) into a generator the
 * runtime consumes. `initialize(change)` wires the source and calls `change(x)`
 * to push each new value; its return (optional) is a dispose fn run when the cell
 * is invalidated (the runtime calls `.return()`).
 */
export function observe(initialize) {
  let stale = false;
  let value;
  let resolve;
  const dispose = initialize(change);

  if (dispose != null && typeof dispose !== "function") {
    throw new Error(
      typeof dispose.then === "function"
        ? "async initializers are not supported"
        : "initializer returned something, but not a dispose function",
    );
  }

  function change(x) {
    if (resolve) resolve(x), (resolve = null);
    else stale = true;
    return (value = x);
  }

  return {
    [Symbol.iterator]() {
      return this;
    },
    throw: () => ({ done: true }),
    return: () => (dispose != null && dispose(), { done: true }),
    next() {
      return {
        done: false,
        value: stale
          ? ((stale = false), Promise.resolve(value))
          : new Promise((_) => (resolve = _)),
      };
    },
  };
}

/**
 * The `viewof` seam: observe an element's value as a generator. Yields the
 * element's current value, then a new value on each `input` event. A `viewof
 * transport` element with a `.value` getter + an `input` dispatch becomes the
 * reactive `head` the whole notebook scrubs against.
 */
export function input(element) {
  return observe((change) => {
    const event = eventof(element);
    const value = valueof(element);
    const inputted = () => change(valueof(element));
    element.addEventListener(event, inputted);
    if (value !== undefined) change(value);
    return () => element.removeEventListener(event, inputted);
  });
}

function valueof(element) {
  switch (element.type) {
    case "range":
    case "number":
      return element.valueAsNumber;
    case "checkbox":
      return element.checked;
    case "select-multiple":
      return Array.from(element.selectedOptions, (o) => o.value);
    default:
      return element.value;
  }
}

function eventof(element) {
  switch (element.type) {
    case "button":
    case "submit":
    case "checkbox":
      return "click";
    case "file":
      return "change";
    default:
      return "input";
  }
}
