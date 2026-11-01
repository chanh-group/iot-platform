import { afterEach, describe, expect, it } from "bun:test";
import { ApiClient, ApiError } from "../src/lib/api";

const realFetch = globalThis.fetch;

function stubFetch(fetcher: (...args: Parameters<typeof fetch>) => Promise<Response>) {
  globalThis.fetch = Object.assign(fetcher, { preconnect: realFetch.preconnect }) as typeof fetch;
}

afterEach(() => {
  globalThis.fetch = realFetch;
});

describe("API client", () => {
  it("sends same-origin credentials and CSRF headers for mutations", async () => {
    let received: RequestInit | undefined;
    stubFetch(async (_input, init) => {
      received = init;
      return new Response(null, { status: 204 });
    });
    const api = new ApiClient(() => {});

    await api.post<void>("/api/v1/auth/logout");

    expect(received?.credentials).toBe("same-origin");
    expect(new Headers(received?.headers).get("X-IoT-CSRF")).toBe("1");
    expect(new Headers(received?.headers).get("Content-Type")).toBe("application/json");
  });

  it("turns malformed successful JSON into invalid_response", async () => {
    stubFetch(async () => new Response("{", {
      status: 200,
      headers: { "X-Request-ID": "request-123" },
    }));
    const api = new ApiClient(() => {});

    await expect(api.get("/api/v1/overview")).rejects.toMatchObject({
      code: "invalid_response",
      requestId: "request-123",
    });
  });

  it("preserves AbortError from fetch", async () => {
    const abort = new DOMException("cancelled", "AbortError");
    stubFetch(async () => { throw abort; });
    const api = new ApiClient(() => {});

    await expect(api.get("/api/v1/overview", new AbortController().signal)).rejects.toBe(abort);
  });

  it("preserves AbortError while reading a response body", async () => {
    const abort = new DOMException("cancelled", "AbortError");
    const response = new Response("ignored");
    Object.defineProperty(response, "text", { value: () => Promise.reject(abort) });
    stubFetch(async () => response);
    const api = new ApiClient(() => {});

    await expect(api.get("/api/v1/overview")).rejects.toBe(abort);
  });

  it("preserves AbortError while reading an error response body", async () => {
    const abort = new DOMException("cancelled", "AbortError");
    const response = new Response("ignored", { status: 401 });
    Object.defineProperty(response, "text", { value: () => Promise.reject(abort) });
    stubFetch(async () => response);
    let expired = 0;
    const api = new ApiClient(() => { expired += 1; });

    await expect(api.get("/api/v1/overview")).rejects.toBe(abort);
    expect(expired).toBe(0);
  });

  it("does not let an older unauthorized response expire a newer session", async () => {
    let resolveFetch!: (response: Response) => void;
    stubFetch(() => new Promise((resolve) => { resolveFetch = resolve; }));
    let expired = 0;
    const api = new ApiClient(() => { expired += 1; });
    api.authenticated();
    const pending = api.get("/api/v1/overview");
    api.authenticated();
    resolveFetch(new Response(JSON.stringify({
      code: "unauthenticated",
      message: "unauthenticated",
      request_id: "stale-request",
    }), { status: 401, headers: { "Content-Type": "application/json" } }));

    await expect(pending).rejects.toBeInstanceOf(ApiError);
    expect(expired).toBe(0);
  });

  it("returns the server request_id with API errors", async () => {
    stubFetch(async () => new Response(JSON.stringify({
      code: "validation_error",
      message: "invalid fields",
      request_id: "api-request-42",
    }), { status: 400, headers: { "Content-Type": "application/json" } }));
    const api = new ApiClient(() => {});

    await expect(api.get("/api/v1/devices")).rejects.toMatchObject({ requestId: "api-request-42" });
  });
});
