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

  acknowledgeSubscription(channels = ["overview"], requestId?: number) {
    const requests = this.sent
      .map((message) => JSON.parse(message) as { action: string; request_id?: number })
      .filter((message) => message.action === "subscribe");
    const id = requestId ?? requests.at(-1)?.request_id;
    this.onmessage?.call(this as unknown as WebSocket, new MessageEvent("message", {
      data: JSON.stringify({ type: "subscribed", channels, request_id: id }),
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

  it("waits for the requested device channel ACK before its initial catch-up", async () => {
    const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: { protocol: "http:", host: "smoke.test" },
    });
    const sockets: FakeSocket[] = [];
    let reconnects = 0;
    let catchups = 0;
    let eventCommittedBetweenAcks = false;
    let catchupSawMissedEvent = false;
    const originalNow = Date.now;
    let now = 1_000;
    Date.now = () => now;
    const client = new EventsSocket({
      channels: ["overview"],
      onEvent: () => {},
      onSubscribed: (channels, reconnected) => {
        if (!reconnected && channels.includes("device:device-a")) {
          catchups += 1;
          catchupSawMissedEvent = eventCommittedBetweenAcks;
        }
      },
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
      client.setChannels(["overview", "device:device-a"]);
      expect(sockets[0]?.sent.map((message) => JSON.parse(message))).toEqual([
        { action: "subscribe", channels: ["overview"], request_id: 1 },
        { action: "subscribe", channels: ["device:device-a"], request_id: 2 },
      ]);
      sockets[0]?.acknowledgeSubscription(["overview"], 1);
      expect(catchups).toBe(0);
      eventCommittedBetweenAcks = true;
      sockets[0]?.acknowledgeSubscription(["device:device-a"], 2);
      expect(catchups).toBe(1);
      expect(catchupSawMissedEvent).toBe(true);
      now = 2_000;
      // The server can drop queued events before this 1013 close reaches the browser.
      sockets[0]?.serverClose(1013);
      await Bun.sleep(700);
      expect(sockets).toHaveLength(2);
      now = 3_000;
      sockets[1]?.open();
      expect(reconnects).toBe(0);
      sockets[1]?.acknowledgeSubscription(["overview", "device:device-a"]);
      expect(reconnects).toBe(1);
      expect(sockets[1]?.sent.map((message) => JSON.parse(message))).toEqual([
        { action: "subscribe", channels: ["overview", "device:device-a"], request_id: 3 },
      ]);
      sockets[1]?.serverClose(1006);
      client.stop();
      await Bun.sleep(700);
      expect(sockets).toHaveLength(2);
    } finally {
      client.stop();
      Date.now = originalNow;
      if (previousLocation) Object.defineProperty(globalThis, "location", previousLocation);
      else Reflect.deleteProperty(globalThis, "location");
    }
  });

  it("ignores a delayed ACK from an earlier open of the same device", async () => {
    const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: { protocol: "http:", host: "smoke.test" },
    });
    const sockets: FakeSocket[] = [];
    const subscribed: string[][] = [];
    let reconnects = 0;
    const client = new EventsSocket({
      channels: ["overview"],
      onEvent: () => {},
      onSubscribed: (channels) => subscribed.push(channels),
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
      sockets[0]?.acknowledgeSubscription(["overview"], 1);
      client.setChannels(["overview", "device:device-a"]);
      client.setChannels(["overview"]);
      client.setChannels(["overview", "device:device-a"]);

      sockets[0]?.acknowledgeSubscription(["device:device-a"], 2);
      expect(subscribed).toEqual([["overview"]]);
      sockets[0]?.acknowledgeSubscription(["device:device-a"], 3);
      expect(subscribed).toEqual([["overview"], ["device:device-a"]]);

      sockets[0]?.serverClose(1013);
      await Bun.sleep(700);
      sockets[1]?.open();
      sockets[1]?.acknowledgeSubscription(["overview", "device:device-a"], 4);
      expect(reconnects).toBe(1);
    } finally {
      client.stop();
      if (previousLocation) Object.defineProperty(globalThis, "location", previousLocation);
      else Reflect.deleteProperty(globalThis, "location");
    }
  });

  it("finishes reconnect after separately acknowledged current channels", async () => {
    const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: { protocol: "http:", host: "smoke.test" },
    });
    const sockets: FakeSocket[] = [];
    const reconnectChannels: string[][] = [];
    const client = new EventsSocket({
      channels: ["overview"],
      onEvent: () => {},
      onReconnect: (channels) => reconnectChannels.push(channels),
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
      sockets[0]?.acknowledgeSubscription(["overview"], 1);
      sockets[0]?.serverClose(1013);
      await Bun.sleep(700);
      sockets[1]?.open();
      client.setChannels(["overview", "device:device-a"]);

      sockets[1]?.acknowledgeSubscription(["overview"], 2);
      expect(reconnectChannels).toEqual([]);
      sockets[1]?.acknowledgeSubscription(["device:device-a"], 3);
      expect(reconnectChannels).toEqual([["overview", "device:device-a"]]);
    } finally {
      client.stop();
      if (previousLocation) Object.defineProperty(globalThis, "location", previousLocation);
      else Reflect.deleteProperty(globalThis, "location");
    }
  });

  it("waits for a new-connection ACK when channels change during reconnect delay", async () => {
    const previousLocation = Object.getOwnPropertyDescriptor(globalThis, "location");
    Object.defineProperty(globalThis, "location", {
      configurable: true,
      value: { protocol: "http:", host: "smoke.test" },
    });
    const sockets: FakeSocket[] = [];
    const reconnectChannels: string[][] = [];
    const client = new EventsSocket({
      channels: ["overview"],
      onEvent: () => {},
      onReconnect: (channels) => reconnectChannels.push(channels),
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
      sockets[0]?.acknowledgeSubscription(["overview"], 1);
      sockets[0]?.serverClose(1013);
      client.setChannels(["overview", "device:device-a"]);
      expect(reconnectChannels).toEqual([]);

      await Bun.sleep(700);
      sockets[1]?.open();
      expect(reconnectChannels).toEqual([]);
      sockets[1]?.acknowledgeSubscription(["overview", "device:device-a"], 2);
      expect(reconnectChannels).toEqual([["overview", "device:device-a"]]);
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
