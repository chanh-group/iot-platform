import { describe, expect, it } from "bun:test";
import { createRoot } from "solid-js";
import { startRequest, useActions } from "../src/lib/useRequest";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => { resolve = done; });
  return { promise, resolve };
}

describe("request lifecycle", () => {
  it("ignores a stale response and aborts it when the key changes", async () => {
    const first = deferred<string>();
    const second = deferred<string>();
    let firstSignal: AbortSignal | undefined;
    let value = "";
    const cancelFirst = startRequest(
      (signal) => {
        firstSignal = signal;
        return first.promise;
      },
      (result) => { value = result; },
      () => {},
    );
    cancelFirst();
    const cancelSecond = startRequest(
      () => second.promise,
      (result) => { value = result; },
      () => {},
    );

    second.resolve("new value");
    await Promise.resolve();
    first.resolve("old value");
    await Promise.resolve();

    expect(firstSignal?.aborted).toBe(true);
    expect(value).toBe("new value");
    cancelSecond();
  });

  it("aborts in-flight work when its request scope is disposed", async () => {
    const pending = deferred<string>();
    let signal: AbortSignal | undefined;
    const dispose = startRequest(
      (currentSignal) => {
        signal = currentSignal;
        return pending.promise;
      },
      () => {},
      () => {},
    );

    dispose();

    expect(signal?.aborted).toBe(true);
  });

  it("aborts an App action when its Solid owner is disposed", async () => {
    const scope = createRoot((dispose) => ({ actions: useActions(), dispose }));
    let signal: AbortSignal | undefined;
    let started!: () => void;
    const requestStarted = new Promise<void>((resolve) => { started = resolve; });
    const pending = scope.actions((currentSignal) => new Promise<never>((_resolve, reject) => {
      signal = currentSignal;
      started();
      currentSignal.addEventListener("abort", () => reject(new DOMException("Aborted", "AbortError")), { once: true });
    }));

    await requestStarted;
    scope.dispose();

    expect(signal?.aborted).toBe(true);
    await expect(pending).rejects.toMatchObject({ name: "AbortError" });
  });
});
