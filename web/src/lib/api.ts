export class ApiError extends Error {
  constructor(
    readonly code: string,
    message: string,
    readonly requestId?: string,
    readonly details?: unknown,
    readonly status?: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

export function isAbortError(error: unknown): boolean {
  return error instanceof DOMException
    ? error.name === "AbortError"
    : error instanceof Error && error.name === "AbortError";
}

function requestIdFrom(response: Response, body: unknown): string | undefined {
  if (typeof body === "object" && body !== null && "request_id" in body) {
    const id = body.request_id;
    if (typeof id === "string") return id;
  }
  return response.headers.get("X-Request-ID") ?? undefined;
}

export class ApiClient {
  private sessionGeneration = 0;

  constructor(private readonly onUnauthorized: () => void) {}

  authenticated(): void {
    this.sessionGeneration += 1;
  }

  unauthenticated(): void {
    this.sessionGeneration += 1;
  }

  async request<T>(
    path: string,
    options: RequestInit = {},
  ): Promise<T> {
    const generation = this.sessionGeneration;
    const method = (options.method ?? "GET").toUpperCase();
    const headers = new Headers(options.headers);
    if (method !== "GET" && method !== "HEAD") {
      headers.set("X-IoT-CSRF", "1");
      if (!headers.has("Content-Type")) {
        headers.set("Content-Type", "application/json");
      }
    }

    let response: Response;
    try {
      response = await fetch(path, {
        ...options,
        method,
        headers,
        credentials: "same-origin",
      });
    } catch (error) {
      if (isAbortError(error)) throw error;
      throw new ApiError("network_error", "The server could not be reached.");
    }

    let text: string;
    try {
      text = response.status === 204 ? "" : await response.text();
    } catch (error) {
      if (isAbortError(error)) throw error;
      throw new ApiError("network_error", "The server response could not be read.");
    }

    let body: unknown;
    if (text !== "") {
      try {
        body = JSON.parse(text) as unknown;
      } catch {
        if (response.ok) {
          throw new ApiError(
            "invalid_response",
            "The server returned invalid JSON.",
            response.headers.get("X-Request-ID") ?? undefined,
            undefined,
            response.status,
          );
        }
      }
    } else if (response.ok && response.status !== 204) {
      throw new ApiError(
        "invalid_response",
        "The server returned an empty response.",
        response.headers.get("X-Request-ID") ?? undefined,
        undefined,
        response.status,
      );
    }

    if (!response.ok) {
      const envelope = typeof body === "object" && body !== null
        ? body as Record<string, unknown>
        : {};
      const code = typeof envelope.code === "string" ? envelope.code : "http_error";
      const message = typeof envelope.message === "string"
        ? envelope.message
        : `Request failed (${response.status}).`;
      const id = requestIdFrom(response, body);
      if (response.status === 401 && generation === this.sessionGeneration) {
        this.onUnauthorized();
      }
      throw new ApiError(code, message, id, envelope.details, response.status);
    }

    return (response.status === 204 ? undefined : body) as T;
  }

  get<T>(path: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>(path, { signal });
  }

  post<T>(path: string, value?: unknown, signal?: AbortSignal): Promise<T> {
    return this.request<T>(path, {
      method: "POST",
      body: value === undefined ? undefined : JSON.stringify(value),
      signal,
    });
  }

  patch<T>(path: string, value: unknown, signal?: AbortSignal): Promise<T> {
    return this.request<T>(path, {
      method: "PATCH",
      body: JSON.stringify(value),
      signal,
    });
  }
}
