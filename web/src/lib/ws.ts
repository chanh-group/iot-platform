import type { RealtimeEvent } from "./models";

export function reconnectDelay(attempt: number): number {
  return Math.min(500 * 2 ** Math.min(Math.max(0, attempt), 20), 10_000);
}

type Socket = Pick<
  WebSocket,
  "readyState" | "onopen" | "onmessage" | "onclose" | "onerror" | "send" | "close"
>;

export interface ReconnectCheckpoint {
  afterId: number;
}

interface WebSocketOptions {
  channels: string[];
  onEvent: (event: RealtimeEvent) => void;
  onSubscribed?: (channels: string[], reconnected: boolean) => void;
  onReconnect: (channels: string[]) => void;
  onDisconnect: () => void;
  onExpired: () => void;
  onStatus: (connected: boolean) => void;
  createSocket?: (url: string) => Socket;
}

export class EventsSocket {
  private socket: Socket | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private stableTimer: ReturnType<typeof setTimeout> | undefined;
  private channels: string[];
  private attempt = 0;
  private stopped = false;
  private openedBefore = false;
  private reconnectPending = false;
  private nextSubscriptionRequestId = 0;
  private readonly pendingSubscriptions = new Map<number, string[]>();
  private readonly latestRequestForChannel = new Map<string, number>();
  private readonly confirmedChannels = new Set<string>();
  private readonly seen = new Set<string>();
  private readonly onOffline = () => this.socket?.close();
  private readonly onOnline = () => {
    if (this.stopped) return;
    if (this.socket?.readyState === 0 || this.socket?.readyState === 1) return;
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    this.socket = undefined;
    this.connect();
  };

  constructor(private readonly options: WebSocketOptions) {
    this.channels = [...new Set(options.channels)];
  }

  start(): void {
    if (typeof window !== "undefined") {
      window.addEventListener("offline", this.onOffline);
      window.addEventListener("online", this.onOnline);
    }
    this.connect();
  }

  setChannels(channels: string[]): void {
    const next = [...new Set(channels)];
    const socket = this.socket;
    const removed = this.channels.filter((channel) => !next.includes(channel));
    const added = next.filter((channel) => !this.channels.includes(channel));
    this.channels = next;
    for (const channel of removed) {
      this.confirmedChannels.delete(channel);
      this.latestRequestForChannel.delete(channel);
    }
    if (socket?.readyState === 1) {
      if (removed.length) this.send({ action: "unsubscribe", channels: removed });
      if (added.length) this.subscribe(added);
    }
    this.finishSubscription();
  }

  stop(): void {
    if (this.stopped) return;
    this.stopped = true;
    if (typeof window !== "undefined") {
      window.removeEventListener("offline", this.onOffline);
      window.removeEventListener("online", this.onOnline);
    }
    if (this.timer !== undefined) clearTimeout(this.timer);
    if (this.stableTimer !== undefined) clearTimeout(this.stableTimer);
    this.timer = undefined;
    this.stableTimer = undefined;
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
    this.pendingSubscriptions.clear();
    this.latestRequestForChannel.clear();
    this.confirmedChannels.clear();
    const url = `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}/api/v1/events`;
    const socket = this.options.createSocket?.(url) ?? new WebSocket(url);
    this.socket = socket;
    socket.onopen = () => {
      if (this.stopped || this.socket !== socket) return;
      this.options.onStatus(false);
      if (this.channels.length) this.subscribe(this.channels);
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
      if (event.type === "subscribed") {
        const acknowledgement = value as { channels?: unknown; request_id?: unknown };
        const requestId = acknowledgement.request_id;
        const requested = typeof requestId === "number" && Number.isSafeInteger(requestId)
          ? this.pendingSubscriptions.get(requestId)
          : undefined;
        if (!requested || typeof requestId !== "number") return;
        this.pendingSubscriptions.delete(requestId);
        const receivedChannels = Array.isArray(acknowledgement.channels)
          ? acknowledgement.channels.filter((channel): channel is string => typeof channel === "string")
          : [];
        const accepted = requested.filter((channel) =>
          receivedChannels.includes(channel)
          && this.channels.includes(channel)
          && this.latestRequestForChannel.get(channel) === requestId,
        );
        for (const channel of accepted) this.confirmedChannels.add(channel);
        if (accepted.length) {
          this.options.onStatus(true);
          this.openedBefore = true;
          this.options.onSubscribed?.(accepted, this.reconnectPending);
        }
        this.finishSubscription();
        return;
      }
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
      this.confirmedChannels.clear();
      this.pendingSubscriptions.clear();
      this.latestRequestForChannel.clear();
      this.options.onStatus(false);
      if (this.stableTimer !== undefined) clearTimeout(this.stableTimer);
      this.stableTimer = undefined;
      if (event.code === 4401) {
        this.options.onExpired();
        return;
      }
      if (this.openedBefore) this.options.onDisconnect();
      this.reconnectPending = true;
      const delay = reconnectDelay(this.attempt++);
      this.timer = setTimeout(() => {
        this.timer = undefined;
        this.connect();
      }, delay);
    };
    socket.onerror = () => socket.close();
  }

  private subscribe(channels: string[]): void {
    const requestId = ++this.nextSubscriptionRequestId;
    const requested = [...channels];
    this.pendingSubscriptions.set(requestId, requested);
    for (const channel of requested) this.latestRequestForChannel.set(channel, requestId);
    this.send({ action: "subscribe", channels: requested, request_id: requestId });
  }

  private finishSubscription(): void {
    if (this.socket?.readyState !== 1
      || !this.channels.length
      || !this.channels.every((channel) => this.confirmedChannels.has(channel))) return;
    if (this.reconnectPending) {
      this.reconnectPending = false;
      this.options.onReconnect([...this.channels]);
    }
    if (this.stableTimer !== undefined) clearTimeout(this.stableTimer);
    this.stableTimer = setTimeout(() => {
      this.stableTimer = undefined;
      this.attempt = 0;
    }, 30_000);
  }

  private send(message: { action: string; channels: string[]; request_id?: number }): void {
    try {
      this.socket?.send(JSON.stringify(message));
    } catch {
      this.socket?.close();
    }
  }
}
