import type { RealtimeEvent } from "./models";

export function reconnectDelay(attempt: number): number {
  return Math.min(500 * 2 ** Math.min(Math.max(0, attempt), 20), 10_000);
}

type Socket = Pick<
  WebSocket,
  "readyState" | "onopen" | "onmessage" | "onclose" | "onerror" | "send" | "close"
>;

interface WebSocketOptions {
  channels: string[];
  onEvent: (event: RealtimeEvent) => void;
  onReconnect: () => void;
  onDisconnect: () => void;
  onExpired: () => void;
  onStatus: (connected: boolean) => void;
  createSocket?: (url: string) => Socket;
}

export class EventsSocket {
  private socket: Socket | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private channels: string[];
  private attempt = 0;
  private stopped = false;
  private openedBefore = false;
  private readonly seen = new Set<string>();

  constructor(private readonly options: WebSocketOptions) {
    this.channels = [...new Set(options.channels)];
  }

  start(): void {
    this.connect();
  }

  setChannels(channels: string[]): void {
    const next = [...new Set(channels)];
    const socket = this.socket;
    if (socket?.readyState === 1) {
      const removed = this.channels.filter((channel) => !next.includes(channel));
      const added = next.filter((channel) => !this.channels.includes(channel));
      if (removed.length) this.send({ action: "unsubscribe", channels: removed });
      if (added.length) this.send({ action: "subscribe", channels: added });
    }
    this.channels = next;
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    const socket = this.socket;
    this.socket = undefined;
    if (socket) {
      socket.onopen = null;
      socket.onmessage = null;
      socket.onclose = null;
      socket.onerror = null;
      socket.close(1000, "view closed");
    }
    this.options.onStatus(false);
  }

  private connect(): void {
    if (this.stopped) return;
    const url = `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/api/v1/events`;
    const socket = this.options.createSocket?.(url) ?? new WebSocket(url);
    this.socket = socket;
    socket.onopen = () => {
      if (this.stopped || this.socket !== socket) return;
      this.attempt = 0;
      this.options.onStatus(true);
      if (this.channels.length) this.send({ action: "subscribe", channels: this.channels });
      if (this.openedBefore) this.options.onReconnect();
      this.openedBefore = true;
    };
    socket.onmessage = (message) => {
      if (this.stopped || this.socket !== socket || typeof message.data !== "string") return;
      let value: unknown;
      try {
        value = JSON.parse(message.data) as unknown;
      } catch {
        return;
      }
      if (typeof value !== "object" || value === null) return;
      const event = value as Partial<RealtimeEvent>;
      if (typeof event.id !== "string" || typeof event.type !== "string") return;
      if (this.seen.has(event.id)) return;
      this.seen.add(event.id);
      if (this.seen.size > 512) {
        const oldest = this.seen.values().next().value as string | undefined;
        if (oldest !== undefined) this.seen.delete(oldest);
      }
      this.options.onEvent({
        id: event.id,
        type: event.type,
        ts: typeof event.ts === "number" ? event.ts : 0,
        device_id: event.device_id,
        data: typeof event.data === "object" && event.data !== null
          ? event.data as Record<string, unknown>
          : {},
      });
    };
    socket.onclose = (event) => {
      if (this.stopped || this.socket !== socket) return;
      this.socket = undefined;
      this.options.onStatus(false);
      if (event.code === 4401) {
        this.options.onExpired();
        return;
      }
      if (this.openedBefore) this.options.onDisconnect();
      const delay = reconnectDelay(this.attempt++);
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.connect();
      }, delay);
    };
    socket.onerror = () => socket.close();
  }

  private send(message: { action: string; channels: string[] }): void {
    try {
      this.socket?.send(JSON.stringify(message));
    } catch {
      this.socket?.close();
    }
  }
}
