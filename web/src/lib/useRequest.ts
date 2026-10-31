import { createEffect, createSignal, onCleanup, type Accessor } from "solid-js";
import { isAbortError } from "./api";

export interface RequestState<T> {
  data?: T;
  loading: boolean;
  error?: unknown;
}

export function startRequest<T>(
  load: (signal: AbortSignal) => Promise<T>,
  onData: (value: T) => void,
  onError: (error: unknown) => void,
): () => void {
  const controller = new AbortController();
  let active = true;
  let pending: Promise<T>;
  try {
    pending = load(controller.signal);
  } catch (error) {
    pending = Promise.reject(error);
  }
  void pending.then(
    (value) => {
      if (active) onData(value);
    },
    (error: unknown) => {
      if (active && !isAbortError(error)) onError(error);
    },
  );
  return () => {
    active = false;
    controller.abort();
  };
}

export function useRequest<T>(
  key: Accessor<unknown>,
  load: (signal: AbortSignal) => Promise<T>,
) {
  const [state, setState] = createSignal<RequestState<T>>({ loading: true });
  const [revision, setRevision] = createSignal(0);

  createEffect(() => {
    key();
    revision();
    setState({ loading: true });
    const cancel = startRequest(
      load,
      (data) => setState({ data, loading: false }),
      (error) => setState({ loading: false, error }),
    );
    onCleanup(cancel);
  });

  return {
    state,
    refresh: () => setRevision((value) => value + 1),
  };
}

export function useActions() {
  const controllers = new Set<AbortController>();
  let disposed = false;
  onCleanup(() => {
    disposed = true;
    for (const controller of controllers) controller.abort();
    controllers.clear();
  });

  return async <T>(work: (signal: AbortSignal) => Promise<T>): Promise<T> => {
    if (disposed) throw new DOMException("Request scope is closed", "AbortError");
    const controller = new AbortController();
    controllers.add(controller);
    try {
      return await work(controller.signal);
    } finally {
      controllers.delete(controller);
    }
  };
}
