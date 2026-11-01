import { describe, expect, it } from "bun:test";
import { EventsSocket, reconnectDelay } from "../src/lib/ws";

class FakeSocket {
  readyState = 0;
  onopen: ((this: WebSocket, event: Event) => unknown) | null = null;
  onmessage: ((this: WebSocket, event: MessageEvent) => unknown) | null = null;
  onclose: ((this: WebSocket, event: CloseEvent) => unknown) | null = null;
  onerror: ((this: WebSocket, event: Event) => unknown) | null = null;
  readonly sent: string[] = [];

  send(value: string) {
    this.sent.push(value);
  }

  close(code = 1000) {
    this.readyState = 3;
    this.onclose?.call(this as unknown as WebSocket, { code } as CloseEvent);
  }

  open() {
    this.readyState = 1;
    this.onopen?.call(this as unknown as WebSocket, new Event("open"));
  }

  acknowledgeSubscription() {
    this.onmessage?.call(this as unknown as WebSocket, new MessageEvent("message", {
      data: JSON.stringify({ type: "subscribed", channels: [] }),
    }));
  }

  serverClose(code: number) {
    this.readyState = 3;
    this.onclose?.call(this as unknown as WebSocket, { code } as CloseEvent);
  }
}

describe("WebSocket reconnect backoff", () => {
  it("grows exponentially and remains bounded", () => {
    expect(reconnectDelay(0)).toBe(500);
    expect(reconnectDelay(1)).toBe(1_000);
    expect(reconnectDelay(5)).toBe(10_000);
    expect(reconnectDelay(100)).toBe(10_000);
    expect(reconnectDelay(-1)).toBe(500);
  });

  it("resubscribes the active channel after reconnect and cancels the next retry on stop", async () => {
    const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: { protocol: "http:", host: "smoke.test" },
    });
    const sockets: FakeSocket[] = [];
    let reconnects = 0;
    const client = new EventsSocket({
      channels: ["overview"],
      onEvent: () => {},
      onReconnect: () => { reconnects += 1; },
      onDisconnect: () => {},
      onExpired: () => {},
      onStatus: () => {},
      createSocket: (url) => {
        expect(url).toBe("ws://smoke.test/api/v1/events");
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
    });

    try {
      client.start();
      sockets[0]?.open();
      client.setChannels(["device:device-a"]);
      expect(sockets[0]?.sent.map((message) => JSON.parse(message))).toEqual([
        { action: "subscribe", channels: ["overview"] },
        { action: "unsubscribe", channels: ["overview"] },
        { action: "subscribe", channels: ["device:device-a"] },
      ]);
      sockets[0]?.acknowledgeSubscription();
      sockets[0]?.serverClose(1006);
      await Bun.sleep(700);
      expect(sockets).toHaveLength(2);
      sockets[1]?.open();
      expect(reconnects).toBe(0);
      sockets[1]?.acknowledgeSubscription();
      expect(reconnects).toBe(1);
      expect(sockets[1]?.sent.map((message) => JSON.parse(message))).toEqual([
        { action: "subscribe", channels: ["device:device-a"] },
      ]);
      sockets[1]?.serverClose(1006);
      client.stop();
      await Bun.sleep(700);
      expect(sockets).toHaveLength(2);
    } finally {
      client.stop();
      if (previousLocation) Object.defineProperty(globalThis, "location", previousLocation);
      else Reflect.deleteProperty(globalThis, "location");
    }
  });

  it("closes on offline and retries immediately when the browser comes back online", () => {
    const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
    const previousWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
    const networkEvents = new EventTarget();
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: { protocol: "http:", host: "smoke.test" },
    });
    Object.defineProperty(globalThis, "window", { configurable: true, value: networkEvents });
    const sockets: FakeSocket[] = [];
    let reconnects = 0;
    const client = new EventsSocket({
      channels: ["overview"],
      onEvent: () => {},
      onReconnect: () => { reconnects += 1; },
      onDisconnect: () => {},
      onExpired: () => {},
      onStatus: () => {},
      createSocket: () => {
        const socket = new FakeSocket();
        sockets.push(socket);
        return socket as unknown as WebSocket;
      },
    });

    try {
      client.start();
      sockets[0]?.open();
      sockets[0]?.acknowledgeSubscription();
      networkEvents.dispatchEvent(new Event("offline"));
      expect(sockets[0]?.readyState).toBe(3);
      networkEvents.dispatchEvent(new Event("online"));
      expect(sockets).toHaveLength(2);
      sockets[1]?.open();
      expect(reconnects).toBe(0);
      sockets[1]?.acknowledgeSubscription();
      expect(reconnects).toBe(1);
    } finally {
      client.stop();
      if (previousWindow) Object.defineProperty(globalThis, "window", previousWindow);
      else Reflect.deleteProperty(globalThis, "window");
      if (previousLocation) Object.defineProperty(globalThis, "location", previousLocation);
      else Reflect.deleteProperty(globalThis, "location");
    }
  });
});
