import { createHash } from "node:crypto";
import { createServer as createTcpServer } from "node:net";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { Database } from "bun:sqlite";
import { expect, it } from "bun:test";
import { createServer } from "vite";
import solid from "vite-plugin-solid";

interface DevtoolsTarget {
  type: string;
  webSocketDebuggerUrl: string;
}

interface DevtoolsReply {
  id?: number;
  method?: string;
  params?: { request?: { url?: string; method?: string } };
  result?: Record<string, unknown>;
  error?: { message: string };
}

interface MqttHelperProcess {
  stdin: { write(value: string): unknown; flush(): unknown };
  stdout: ReadableStream<Uint8Array>;
  exited: Promise<number>;
  kill(): void;
}

interface MqttHelperMessage {
  event?: string;
  id?: number;
  code?: number;
  topic?: string;
  payload?: string;
  ok?: boolean;
  error?: string;
  message?: string;
}

function withTimeout<T>(promise: Promise<T>, timeoutMs: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`Timed out waiting for ${label}.`)), timeoutMs);
    }),
  ]).finally(() => clearTimeout(timer));
}

function contrastRatio(foreground: string, background: string): number {
  const luminance = (color: string) => {
    const channels = color.match(/[\d.]+/g)?.slice(0, 3).map(Number);
    if (!channels || channels.length !== 3) throw new Error(`Unexpected computed color: ${color}`);
    const linear = channels.map((channel) => {
      const value = channel / 255;
      return value <= 0.04045 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4;
    });
    return linear[0]! * 0.2126 + linear[1]! * 0.7152 + linear[2]! * 0.0722;
  };
  const values = [luminance(foreground), luminance(background)].sort((left, right) => right - left);
  return (values[0]! + 0.05) / (values[1]! + 0.05);
}

class MqttDevice {
  private nextId = 0;
  private readonly pending = new Map<number, { resolve: (message: MqttHelperMessage) => void; reject: (error: Error) => void }>();
  private readonly publications: Array<{ topic: string; payload: string }> = [];
  private readonly publicationWaiters: Array<{
    topic: string;
    resolve: (payload: Record<string, unknown>) => void;
    reject: (error: Error) => void;
    timer: ReturnType<typeof setTimeout>;
  }> = [];
  private readonly ready: Promise<number>;
  private resolveReady!: (code: number) => void;
  private rejectReady!: (error: Error) => void;
  private readySettled = false;
  private ended = false;
  private readonly closedPromise: Promise<void>;
  private resolveClosed!: () => void;

  private constructor(private readonly process: MqttHelperProcess) {
    this.ready = new Promise((resolve, reject) => {
      this.resolveReady = resolve;
      this.rejectReady = reject;
    });
    this.closedPromise = new Promise((resolve) => { this.resolveClosed = resolve; });
    void this.readOutput().catch((error: unknown) => this.fail(error instanceof Error ? error : new Error(String(error))));
    void process.exited.then(() => this.finish());
  }

  static async connect(helper: string, host: string, port: number, deviceId: string, token: string): Promise<{ client?: MqttDevice; code: number }> {
    const child = Bun.spawn([helper, host, String(port), deviceId, token], {
      stdin: "pipe",
      stdout: "pipe",
      stderr: "ignore",
    }) as unknown as MqttHelperProcess;
    const client = new MqttDevice(child);
    try {
      const code = await withTimeout(client.ready, 10_000, "the rumqttc helper to connect");
      if (code !== 0) {
        await child.exited;
        return { code };
      }
      return { client, code };
    } catch (error) {
      child.kill();
      await child.exited;
      throw error;
    }
  }

  async subscribe(topic: string): Promise<void> {
    const response = await this.command({ action: "subscribe", topic });
    if (!response.ok) throw new Error(`rumqttc subscription was rejected for ${topic}.`);
  }

  async publish(topic: string, payload: unknown): Promise<void> {
    await this.command({ action: "publish", topic, payload });
  }

  async waitPublish(topic: string, timeoutMs = 10_000): Promise<Record<string, unknown>> {
    const index = this.publications.findIndex((item) => item.topic === topic);
    if (index >= 0) return JSON.parse(this.publications.splice(index, 1)[0]!.payload) as Record<string, unknown>;
    if (this.ended) throw new Error("MQTT helper ended before the expected publication.");
    return new Promise((resolve, reject) => {
      const waiter = {
        topic,
        resolve,
        reject,
        timer: setTimeout(() => {
          const waitingIndex = this.publicationWaiters.indexOf(waiter);
          if (waitingIndex >= 0) this.publicationWaiters.splice(waitingIndex, 1);
          reject(new Error(`Timed out waiting for MQTT publication on ${topic}.`));
        }, timeoutMs),
      };
      this.publicationWaiters.push(waiter);
    });
  }

  async close(): Promise<void> {
    if (!this.ended) {
      try {
        await this.command({ action: "close" });
      } catch {
        this.process.kill();
      }
    }
    if (!await Promise.race([this.process.exited.then(() => true), Bun.sleep(2_000).then(() => false)])) {
      this.process.kill();
      await this.process.exited;
    }
  }

  get closedConnection(): Promise<void> {
    return this.closedPromise;
  }

  private async command(command: Record<string, unknown>): Promise<MqttHelperMessage> {
    if (this.ended) throw new Error("MQTT helper has already exited.");
    const id = ++this.nextId;
    const response = new Promise<MqttHelperMessage>((resolve, reject) => this.pending.set(id, { resolve, reject }));
    this.process.stdin.write(`${JSON.stringify({ ...command, id })}\n`);
    await this.process.stdin.flush();
    try {
      const result = await withTimeout(response, 10_000, `MQTT helper action ${String(command.action)}`);
      if (result.error) throw new Error(result.error);
      return result;
    } catch (error) {
      this.pending.delete(id);
      throw error;
    }
  }

  private async readOutput(): Promise<void> {
    const reader = this.process.stdout.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    try {
      while (true) {
        const { value, done } = await reader.read();
        buffered += decoder.decode(value, { stream: !done });
        let newline = buffered.indexOf("\n");
        while (newline >= 0) {
          this.receive(JSON.parse(buffered.slice(0, newline)) as MqttHelperMessage);
          buffered = buffered.slice(newline + 1);
          newline = buffered.indexOf("\n");
        }
        if (done) break;
      }
      if (buffered.trim()) this.receive(JSON.parse(buffered) as MqttHelperMessage);
    } finally {
      reader.releaseLock();
      this.finish();
    }
  }

  private receive(message: MqttHelperMessage): void {
    if (message.event === "connack") {
      this.readySettled = true;
      this.resolveReady(message.code ?? 255);
      return;
    }
    if (message.event === "error") {
      this.fail(new Error(message.message ?? "rumqttc helper failed."));
      return;
    }
    if (message.event === "disconnected") {
      this.finish();
      return;
    }
    if (message.event === "publish" && message.topic !== undefined && message.payload !== undefined) {
      const index = this.publicationWaiters.findIndex((waiter) => waiter.topic === message.topic);
      const waiter = index < 0 ? undefined : this.publicationWaiters.splice(index, 1)[0];
      if (waiter) {
        clearTimeout(waiter.timer);
        waiter.resolve(JSON.parse(message.payload) as Record<string, unknown>);
      } else {
        this.publications.push({ topic: message.topic, payload: message.payload });
      }
      return;
    }
    if (message.id !== undefined) {
      const waiting = this.pending.get(message.id);
      if (!waiting) return;
      this.pending.delete(message.id);
      waiting.resolve(message);
    }
  }

  private fail(error: Error): void {
    if (!this.readySettled) {
      this.readySettled = true;
      this.rejectReady(error);
    }
    for (const waiting of this.pending.values()) waiting.reject(error);
    this.pending.clear();
    for (const waiter of this.publicationWaiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.finish();
  }

  private finish(): void {
    if (this.ended) return;
    this.ended = true;
    if (!this.readySettled) {
      this.readySettled = true;
      this.rejectReady(new Error("rumqttc helper exited before connecting."));
    }
    for (const waiting of this.pending.values()) waiting.reject(new Error("rumqttc helper exited."));
    this.pending.clear();
    for (const waiter of this.publicationWaiters.splice(0)) {
      clearTimeout(waiter.timer);
      waiter.reject(new Error("rumqttc helper exited before the expected publication."));
    }
    this.resolveClosed();
  }
}

it("drives the Solid dashboard against the Rust API and MQTT broker", async () => {
  const root = resolve(import.meta.dir, "../..");
  const temp = await mkdtemp(join(tmpdir(), "iot-dashboard-backend-"));
  const profile = join(temp, "chrome-profile");
  const dataDir = join(temp, "data");
  const dbPath = join(dataDir, "iot.db");
  const configPath = join(temp, "conf.toml");
  const passwordPath = join(temp, "admin-password");
  const mqttHelper = join(root, "target/debug/examples/backend_ui_mqtt");
  const password = "dashboard-test-password";
  const httpPort = await freePort();
  const mqttPort = await freePort();
  let server: ReturnType<typeof Bun.spawn> | undefined;
  let chrome: ReturnType<typeof Bun.spawn> | undefined;
  let vite: Awaited<ReturnType<typeof createServer>> | undefined;
  let devtools: WebSocket | undefined;
  let mqtt: MqttDevice | undefined;
  let rotatedMqtt: MqttDevice | undefined;
  try {
    await writeFile(passwordPath, `${password}\n`);
    await writeFile(configPath, `config_version = 1

[server]
data_dir = "./data"

[http]
listen = "127.0.0.1:${httpPort}"

[mqtt]
listen = "127.0.0.1:${mqttPort}"

[database]
url = "sqlite://./data/iot.db"
`);

    const build = Bun.spawn(["cargo", "build", "--quiet", "--bin", "iot-server", "--example", "backend_ui_mqtt"], {
      cwd: root,
      stdout: "ignore",
      stderr: "ignore",
    });
    if (await build.exited !== 0) throw new Error("Could not build the Rust server for the browser integration test.");
    const binary = join(root, "target/debug/iot-server");
    const migrate = Bun.spawn([binary, "--config", configPath, "migrate"], {
      cwd: root,
      stdout: "ignore",
      stderr: "pipe",
    });
    const [migrationStatus, migrationError] = await Promise.all([migrate.exited, new Response(migrate.stderr).text()]);
    if (migrationStatus !== 0) throw new Error(`Could not migrate the temporary dashboard database: ${migrationError.trim()}`);
    const admin = Bun.spawn([binary, "--config", configPath, "admin", "create", "--username", "ui-admin", "--password-file", passwordPath], {
      cwd: root,
      stdout: "ignore",
      stderr: "pipe",
    });
    const [adminStatus, adminError] = await Promise.all([admin.exited, new Response(admin.stderr).text()]);
    if (adminStatus !== 0) throw new Error(`Could not create the temporary dashboard test account: ${adminError.trim()}`);
    server = Bun.spawn([binary, "--config", configPath, "run"], {
      cwd: root,
      stdout: "ignore",
      stderr: "ignore",
    });
    await waitFor(async () => {
      try {
        return (await fetch(`http://127.0.0.1:${httpPort}/readyz`)).ok;
      } catch {
        return false;
      }
    }, "Rust API readiness", 20_000);

    vite = await createServer({
      configFile: false,
      root: join(root, "web"),
      plugins: [solid()],
      server: {
        host: "127.0.0.1",
        port: 0,
        strictPort: true,
        hmr: false,
        proxy: { "/api": { target: `http://127.0.0.1:${httpPort}`, changeOrigin: false, ws: true } },
      },
    });
    await vite.listen();
    const address = vite.httpServer?.address();
    if (!address || typeof address === "string") throw new Error("Vite did not open a browser-test listener.");

    chrome = Bun.spawn([
      process.env.CHROME_BIN ?? "google-chrome",
      "--headless=new",
      "--no-sandbox",
      "--disable-dev-shm-usage",
      "--no-first-run",
      "--remote-allow-origins=*",
      "--remote-debugging-port=0",
      `--user-data-dir=${profile}`,
      "about:blank",
    ], { stdout: "ignore", stderr: "ignore" });
    const activePortFile = join(profile, "DevToolsActivePort");
    await waitFor(async () => await Bun.file(activePortFile).exists(), "Chrome DevTools port", 10_000);
    const devtoolsPort = (await Bun.file(activePortFile).text()).split("\n")[0];
    if (!devtoolsPort) throw new Error("Chrome did not publish its remote debugging port.");
    let target: DevtoolsTarget | undefined;
    await waitFor(async () => {
      const targets = await fetch(`http://127.0.0.1:${devtoolsPort}/json/list`).then((response) => response.json()) as DevtoolsTarget[];
      target = targets.find((candidate) => candidate.type === "page");
      return Boolean(target);
    }, "Chrome page target", 10_000);
    if (!target) throw new Error("Chrome did not expose a page target.");

    devtools = new WebSocket(target.webSocketDebuggerUrl);
    await new Promise<void>((resolveOpen, reject) => {
      devtools!.addEventListener("open", () => resolveOpen(), { once: true });
      devtools!.addEventListener("error", () => reject(new Error("Could not connect to Chrome DevTools.")), { once: true });
    });
    const pending = new Map<number, { resolve: (reply: DevtoolsReply) => void; reject: (error: Error) => void }>();
    const requestUrls: string[] = [];
    const requestMethods: string[] = [];
    let nextId = 0;
    devtools.addEventListener("message", (message) => {
      const reply = JSON.parse(String(message.data)) as DevtoolsReply;
      const url = reply.params?.request?.url;
      if (reply.method === "Network.requestWillBeSent" && url) {
        requestUrls.push(url);
        requestMethods.push(`${reply.params?.request?.method} ${new URL(url).pathname}`);
      }
      if (reply.id === undefined) return;
      const waiting = pending.get(reply.id);
      if (!waiting) return;
      pending.delete(reply.id);
      if (reply.error) waiting.reject(new Error(reply.error.message));
      else waiting.resolve(reply);
    });
    const call = (method: string, params: Record<string, unknown> = {}) => new Promise<DevtoolsReply>((resolveReply, reject) => {
      const id = ++nextId;
      pending.set(id, { resolve: resolveReply, reject });
      devtools!.send(JSON.stringify({ id, method, params }));
    });
    const evaluate = async (expression: string): Promise<unknown> => {
      const reply = await call("Runtime.evaluate", { expression, returnByValue: true, awaitPromise: true });
      const details = reply.result?.exceptionDetails as { text?: string } | undefined;
      if (details) throw new Error(`Browser evaluation failed: ${details.text ?? "unknown exception"}`);
      return (reply.result?.result as { value?: unknown } | undefined)?.value;
    };
    const waitForBrowser = async (expression: string, expected: (value: unknown) => boolean, label: string, timeoutMs = 15_000) => {
      try {
        await waitFor(async () => expected(await evaluate(expression)), label, timeoutMs);
      } catch (error) {
        const visibleText = await evaluate("document.body.innerText") as string;
        const redacted = visibleText.replace(/iot_[A-Za-z0-9_-]{20,}/g, "[redacted token]").slice(-2_000);
        throw new Error(`${error instanceof Error ? error.message : "Browser wait failed."} Visible page text: ${redacted}`);
      }
    };
    const click = async (label: string) => {
      const clicked = await evaluate(`(() => {
      const button = [...document.querySelectorAll('button')].find((item) => item.textContent?.trim() === ${JSON.stringify(label)});
      button?.click();
      return Boolean(button);
      })()`);
      if (clicked) return true;
      const buttons = await evaluate("[...document.querySelectorAll('button')].map((button) => button.textContent?.trim())");
      const visible = await evaluate("document.body.innerText") as string;
      throw new Error(`Could not find button ${JSON.stringify(label)}. Buttons: ${JSON.stringify(buttons)}. Page: ${visible.replace(/iot_[A-Za-z0-9_-]{20,}/g, "[redacted token]").slice(-1_000)}`);
    };
    const setValue = (selector: string, value: string, event = "input") => evaluate(`(() => {
      const field = document.querySelector(${JSON.stringify(selector)});
      if (!field) return false;
      field.value = ${JSON.stringify(value)};
      field.dispatchEvent(new Event(${JSON.stringify(event)}, { bubbles: true }));
      return true;
    })()`);
    const enter = async () => {
      await call("Input.dispatchKeyEvent", { type: "keyDown", key: "Enter", code: "Enter", text: "\r", unmodifiedText: "\r", windowsVirtualKeyCode: 13 });
      await call("Input.dispatchKeyEvent", { type: "keyUp", key: "Enter", code: "Enter", windowsVirtualKeyCode: 13 });
    };
    const space = async () => {
      await call("Input.dispatchKeyEvent", { type: "keyDown", key: " ", code: "Space", text: " ", unmodifiedText: " ", windowsVirtualKeyCode: 32 });
      await call("Input.dispatchKeyEvent", { type: "keyUp", key: " ", code: "Space", windowsVirtualKeyCode: 32 });
    };
    const escape = async () => {
      await call("Input.dispatchKeyEvent", { type: "keyDown", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
      await call("Input.dispatchKeyEvent", { type: "keyUp", key: "Escape", code: "Escape", windowsVirtualKeyCode: 27 });
    };
    const tab = async (shift = false) => {
      const modifiers = shift ? 8 : 0;
      await call("Input.dispatchKeyEvent", { type: "rawKeyDown", key: "Tab", code: "Tab", modifiers, windowsVirtualKeyCode: 9 });
      await call("Input.dispatchKeyEvent", { type: "keyUp", key: "Tab", code: "Tab", modifiers, windowsVirtualKeyCode: 9 });
    };
    const focusableCount = `(() => {
      const scope = document.querySelector('dialog[open]') ?? document;
      return [...scope.querySelectorAll('a[href],button,input:not([type="hidden"]),select,textarea,[tabindex]')].filter((element) => {
        const style = getComputedStyle(element);
        return element.tabIndex >= 0 && !element.matches(':disabled') && style.display !== 'none' && style.visibility !== 'hidden' && element.getClientRects().length > 0;
      }).length;
    })()`;
    const startKeyboardTraversal = () => evaluate(`(() => {
      const dialog = document.querySelector('dialog[open]');
      if (dialog) dialog.querySelector('textarea,button,input,select')?.focus();
      else {
        document.body.tabIndex = -1;
        document.body.focus();
      }
      return {
        isModal: dialog?.matches(':modal') ?? false,
        active: document.activeElement instanceof HTMLElement ? document.activeElement.outerHTML.slice(0, 160) : String(document.activeElement),
      };
    })()`);
    const tabToControl = async (predicate: string, label: string) => {
      const count = await evaluate(focusableCount) as number;
      await startKeyboardTraversal();
      for (let index = 0; index < count * 8; index += 1) {
        await tab();
        if (await evaluate(`(() => { const active = document.activeElement; return ${predicate}; })()`)) {
          await evaluate("document.body.removeAttribute('tabindex')");
          return;
        }
      }
      await evaluate("document.body.removeAttribute('tabindex')");
      throw new Error(`Keyboard Tab could not reach ${label}.`);
    };
    const auditKeyboard = async (label: string) => {
      const report = await evaluate(`(() => {
        const scope = document.querySelector('dialog[open]') ?? document;
        const controls = [...scope.querySelectorAll('a[href],button,input:not([type="hidden"]),select,textarea,[tabindex]')].filter((element) => {
          const style = getComputedStyle(element);
          return element.tabIndex >= 0 && !element.matches(':disabled') && style.display !== 'none' && style.visibility !== 'hidden' && element.getClientRects().length > 0;
        });
        const accessibleName = (element) => {
          const labelledBy = element.getAttribute('aria-labelledby')?.split(/\\s+/).map((id) => document.getElementById(id)?.textContent?.trim() ?? '').filter(Boolean).join(' ');
          const labels = 'labels' in element ? [...(element.labels ?? [])].map((item) => item.textContent?.trim() ?? '').filter(Boolean).join(' ') : '';
          return element.getAttribute('aria-label')?.trim() || labelledBy || labels || element.getAttribute('title')?.trim() || element.innerText?.trim() || element.textContent?.trim() || element.value?.trim() || '';
        };
        const tableName = (table) => table.getAttribute('aria-label')?.trim()
          || table.getAttribute('aria-labelledby')?.split(/\\s+/).map((id) => document.getElementById(id)?.textContent?.trim() ?? '').filter(Boolean).join(' ')
          || table.querySelector('caption')?.textContent?.trim()
          || '';
        return {
          count: controls.length,
          names: controls.map(accessibleName),
          unlabeled: controls.filter((element) => !accessibleName(element)).map((element) => element.outerHTML.slice(0, 160)),
          unnamedTables: [...scope.querySelectorAll('table')].filter((table) => !table.closest('[role="img"]') && !tableName(table)).map((table) => table.outerHTML.slice(0, 160)),
          mainCount: document.querySelectorAll('main').length,
          h1Count: document.querySelectorAll('h1').length,
          unnamedNavs: [...document.querySelectorAll('nav')].filter((nav) => !nav.getAttribute('aria-label') && !nav.getAttribute('aria-labelledby')).length,
        };
      })()` ) as { count: number; names: string[]; unlabeled: string[]; unnamedTables: string[]; mainCount: number; h1Count: number; unnamedNavs: number };
      if (report.count === 0) throw new Error(`${label} has no keyboard controls to audit.`);
      if (report.unlabeled.length) throw new Error(`${label} has unnamed controls: ${JSON.stringify(report.unlabeled)}`);
      if (report.unnamedTables.length) throw new Error(`${label} has unnamed data tables: ${JSON.stringify(report.unnamedTables)}`);
      if (report.mainCount !== 1 || report.h1Count !== 1 || report.unnamedNavs !== 0) {
        throw new Error(`${label}: expected one main landmark, one page heading, and named navigation; got ${JSON.stringify(report)}.`);
      }

      const traversalStart = await startKeyboardTraversal();
      const visited = new Set<number>();
      const order: number[] = [];
      for (let step = 0; step < report.count * 8 && visited.size < report.count; step += 1) {
        await tab();
        const focus = await evaluate(`(() => {
          const scope = document.querySelector('dialog[open]') ?? document;
          const controls = [...scope.querySelectorAll('a[href],button,input:not([type="hidden"]),select,textarea,[tabindex]')].filter((element) => {
            const style = getComputedStyle(element);
            return element.tabIndex >= 0 && !element.matches(':disabled') && style.display !== 'none' && style.visibility !== 'hidden' && element.getClientRects().length > 0;
          });
          return {
            index: controls.indexOf(document.activeElement),
            active: document.activeElement instanceof HTMLElement ? document.activeElement.outerHTML.slice(0, 160) : String(document.activeElement),
          };
        })()` ) as { index: number; active: string };
        if (focus.index < 0) throw new Error(`${label}: Tab ${step + 1}/${report.count} left the page's focusable controls (${focus.active}); start=${JSON.stringify(traversalStart)}, order=${JSON.stringify(order)}, names=${JSON.stringify(report.names)}.`);
        visited.add(focus.index);
        order.push(focus.index);
      }
      if (visited.size !== report.count) {
        throw new Error(`${label}: Tab traversal reached ${visited.size}/${report.count} controls; order=${JSON.stringify(order)}; names=${JSON.stringify(report.names)}.`);
      }
      if (await evaluate("Boolean(document.querySelector('dialog[open]'))")) {
        await tab(true);
        const reverseIndex = await evaluate(`(() => {
          const dialog = document.querySelector('dialog[open]');
          const controls = [...dialog.querySelectorAll('a[href],button,input:not([type="hidden"]),select,textarea,[tabindex]')].filter((element) => element.tabIndex >= 0 && !element.matches(':disabled') && element.getClientRects().length > 0);
          return controls.indexOf(document.activeElement);
        })()`);
        if (reverseIndex !== report.count - 1) throw new Error(`${label}: Shift+Tab did not wrap to the last dialog control.`);
        await tab();
        const forwardIndex = await evaluate(`(() => {
          const dialog = document.querySelector('dialog[open]');
          const controls = [...dialog.querySelectorAll('a[href],button,input:not([type="hidden"]),select,textarea,[tabindex]')].filter((element) => element.tabIndex >= 0 && !element.matches(':disabled') && element.getClientRects().length > 0);
          return controls.indexOf(document.activeElement);
        })()`);
        if (forwardIndex !== 0) throw new Error(`${label}: Tab did not wrap from the last to the first dialog control.`);
      } else {
        await evaluate("document.body.removeAttribute('tabindex')");
      }
    };
    const reconnectOffline = async () => {
      await call("Network.emulateNetworkConditions", { offline: true, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
      await evaluate("window.__iotSockets.at(-1)?.close(4000, 'integration disconnect')");
      await waitForBrowser("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates reconnecting", "WebSocket disconnect");
      await call("Network.emulateNetworkConditions", { offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
      await waitForBrowser("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates connected", "WebSocket reconnect");
    };

    await call("Page.enable");
    await call("Runtime.enable");
    await call("Network.enable");
    await call("Page.bringToFront");
    await call("Page.addScriptToEvaluateOnNewDocument", {
      source: `(() => {
        const sockets = [];
        window.__iotSockets = sockets;
        const stableTimers = new Set();
        const overviewIntervals = new Set();
        window.__iotStableTimers = stableTimers;
        window.__iotOverviewIntervals = overviewIntervals;
        const nativeSetTimeout = window.setTimeout.bind(window);
        const nativeClearTimeout = window.clearTimeout.bind(window);
        const nativeSetInterval = window.setInterval.bind(window);
        const nativeClearInterval = window.clearInterval.bind(window);
        window.setTimeout = (callback, delay, ...args) => {
          let timer;
          const tracked = delay === 30000;
          timer = nativeSetTimeout((...callbackArgs) => {
            if (tracked) stableTimers.delete(timer);
            callback(...callbackArgs);
          }, delay, ...args);
          if (tracked) stableTimers.add(timer);
          return timer;
        };
        window.clearTimeout = (timer) => {
          stableTimers.delete(timer);
          return nativeClearTimeout(timer);
        };
        window.setInterval = (callback, delay, ...args) => {
          const timer = nativeSetInterval(callback, delay, ...args);
          if (delay === 15000) overviewIntervals.add(timer);
          return timer;
        };
        window.clearInterval = (timer) => {
          overviewIntervals.delete(timer);
          return nativeClearInterval(timer);
        };
        window.WebSocket = new Proxy(window.WebSocket, {
          construct(target, args) {
            const socket = Reflect.construct(target, args);
            sockets.push(socket);
            return socket;
          },
        });
      })();`,
    });
    await call("Page.navigate", { url: `http://127.0.0.1:${address.port}` });
    await waitForBrowser("document.querySelector('form[aria-label=\"Admin login\"]') !== null", Boolean, "login form");
    const primaryColors = await evaluate(`(() => {
      const style = getComputedStyle(document.querySelector('.button-primary'));
      return { foreground: style.color, background: style.backgroundColor };
    })()`) as { foreground: string; background: string };
    expect(contrastRatio(primaryColors.foreground, primaryColors.background)).toBeGreaterThanOrEqual(4.5);
    await auditKeyboard("admin login");
    expect(await evaluate(`([...document.querySelectorAll('input,select,textarea')].every((field) => field.labels?.length || field.getAttribute('aria-label')) && [...document.querySelectorAll('button')].every((button) => button.getAttribute('aria-label') || button.textContent?.trim()))`)).toBe(true);
    await setValue("input[name='username']", "ui-admin");
    await setValue("input[name='password']", password);
    await tabToControl("active.matches('form[aria-label=\\\"Admin login\\\"] button[type=\\\"submit\\\"]')", "login submit");
    await enter();
    await waitForBrowser("document.querySelector('#overview-title')?.textContent?.trim()", (value) => value === "System overview", "authenticated overview");
    await waitForBrowser("document.activeElement?.id === 'overview-title'", Boolean, "focus moved to overview after login");
    await auditKeyboard("overview");
    await waitForBrowser("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates connected", "authenticated WebSocket");
    expect(await evaluate("document.querySelector('.connection')?.getAttribute('role')")).toBe("status");
    await waitForBrowser("window.__iotStableTimers.size === 1 && window.__iotOverviewIntervals.size === 1", Boolean, "overview timers started");
    expect(await evaluate("['Devices online', 'Telemetry events · 24h', 'Commands in progress', 'Ingest queue'].every((label) => document.body.innerText.includes(label))")).toBe(true);
    expect(await evaluate("localStorage.length")).toBe(0);

    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Devices'", "Devices navigation");
    await space();
    await waitForBrowser("document.querySelector('#devices-title')?.textContent?.trim()", (value) => value === "Devices", "device list");
    await waitForBrowser("document.activeElement?.id === 'devices-title'", Boolean, "focus moved to Devices after navigation");
    await auditKeyboard("device list");
    await waitForBrowser("window.__iotOverviewIntervals.size === 0", Boolean, "overview interval cleanup");
    const deviceName = `browser-${Date.now()}`;
    await setValue("#new-device-name", deviceName);
    await setValue("#new-device-metadata", "[]");
    await tabToControl("active.matches('#new-device-submit')", "Create device submit");
    await enter();
    await waitForBrowser("document.querySelector('[role=alert]')?.textContent", (value) => typeof value === "string" && value.includes("Metadata must be a JSON object"), "create-device validation error");
    await setValue("#new-device-metadata", JSON.stringify({ room: "lab" }));
    await tabToControl("active.matches('#new-device-submit')", "Create device submit");
    await enter();
    await waitForBrowser("document.querySelector('.token-dialog[open]') !== null", Boolean, "one-time device token");
    expect(await evaluate("document.querySelector('.token-dialog')?.getAttribute('aria-describedby')")).toBe("token-description");
    await auditKeyboard("one-time token dialog");
    const deviceId = await evaluate(`document.querySelector('.table-wrap tbody tr')?.querySelector('small')?.textContent?.trim()`) as string;
    const initialToken = await evaluate("document.querySelector('#issued-token')?.value") as string;
    await waitForBrowser(`[...document.querySelectorAll('button')].some((button) => button.textContent?.trim() === ${JSON.stringify(deviceName)})`, Boolean, "new device appears in the list");
    expect(deviceId).toMatch(/^[0-9a-f-]{36}$/);
    expect(initialToken).toMatch(/^iot_[A-Za-z0-9_-]{43}$/);
    await escape();
    await waitForBrowser("document.querySelector('#issued-token') === null", Boolean, "token cleared on close");
    await Bun.sleep(100);
    const createFocus = await evaluate(`(() => {
      const target = document.getElementById('new-device-submit');
      return { activeId: document.activeElement?.id, activeTag: document.activeElement?.tagName, target: target?.outerHTML, targetFocused: target === document.activeElement };
    })()`);
    if ((createFocus as { targetFocused?: boolean }).targetFocused !== true) {
      throw new Error(`Token close did not restore the create button focus: ${JSON.stringify(createFocus)}`);
    }

    await tabToControl("active.matches('#new-device-submit')", "Create device submit");
    await setValue("#new-device-name", deviceName);
    await setValue("#new-device-metadata", "{}");
    await tabToControl("active.matches('#new-device-submit')", "Create device submit");
    await enter();
    await waitForBrowser("document.querySelector('[role=alert]')?.textContent", (value) => typeof value === "string" && value.includes("device name already exists"), "duplicate device error");
    const createAttemptsBeforeBlock = requestMethods.filter((request) => request === "POST /api/v1/devices").length;
    await call("Network.setBlockedURLs", { urls: ["*://*/api/v1/devices*"] });
    await setValue("#new-device-name", `${deviceName}-uncertain`);
    await setValue("#new-device-metadata", "{}");
    await tabToControl("active.matches('#new-device-submit')", "Create device submit");
    await enter();
    await waitForBrowser("document.querySelector('[role=alert]')?.textContent", (value) => typeof value === "string" && value.includes("check the current state before trying again"), "create network-error guidance");
    await Bun.sleep(700);
    expect(requestMethods.filter((request) => request === "POST /api/v1/devices")).toHaveLength(createAttemptsBeforeBlock + 1);
    await call("Network.setBlockedURLs", { urls: [] });

    const connected = await MqttDevice.connect(mqttHelper, "127.0.0.1", mqttPort, deviceId, initialToken);
    if (connected.code !== 0 || !connected.client) throw new Error(`MQTT device connection failed with code ${connected.code}.`);
    const deviceMqtt = connected.client;
    mqtt = deviceMqtt;
    await deviceMqtt.subscribe(`devices/${deviceId}/telemetry/ack`);
    await deviceMqtt.subscribe(`devices/${deviceId}/commands`);

    await click("Refresh list");
    await waitForBrowser(`document.querySelector('.table-wrap tbody')?.innerText.includes(${JSON.stringify(deviceName)})`, Boolean, "created device list refresh");
    await tabToControl(`active instanceof HTMLButtonElement && active.textContent?.trim() === ${JSON.stringify(deviceName)}`, "open device detail");
    await enter();
    await waitForBrowser("document.querySelector('#device-title')?.textContent?.trim()", (value) => value === "Device detail", "device detail");
    await waitForBrowser("document.activeElement?.id === 'device-title'", Boolean, "focus moved to device detail");
    await auditKeyboard("device detail");
    expect(await evaluate(`([...document.querySelectorAll('input,select,textarea')].every((field) => field.labels?.length || field.getAttribute('aria-label')) && [...document.querySelectorAll('button')].every((button) => button.getAttribute('aria-label') || button.textContent?.trim()))`)).toBe(true);
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Back to devices'", "Back to devices");
    await enter();
    await waitForBrowser("document.querySelector('#devices-title') !== null && document.activeElement?.id === 'devices-title'", Boolean, "keyboard back to devices");
    await tabToControl(`active instanceof HTMLButtonElement && active.textContent?.trim() === ${JSON.stringify(deviceName)}`, "reopen device detail");
    await enter();
    await waitForBrowser("document.querySelector('#device-title') !== null && document.activeElement?.id === 'device-title'", Boolean, "keyboard reopen device detail");
    const historyToInput = await evaluate(`(() => {
      const date = new Date(Date.now());
      const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
      return local.toISOString().slice(0, 23);
    })()`) as string;
    await setValue("#history-to", historyToInput);
    const timestamp = Date.now() - 1_000;
    const historyFromInput = await evaluate(`(() => {
      const date = new Date(${timestamp - 1_000});
      const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
      return local.toISOString().slice(0, 23);
    })()`) as string;
    await setValue("#history-from", historyFromInput);
    const firstEvent = { event_id: `ui-${timestamp}`, ts: timestamp, values: { temperature: 22, humidity: 61, relay: true } };
    await deviceMqtt.publish(`devices/${deviceId}/telemetry`, firstEvent);
    expect((await deviceMqtt.waitPublish(`devices/${deviceId}/telemetry/ack`)).status).toBe("stored");
    await deviceMqtt.publish(`devices/${deviceId}/attributes`, { ts: timestamp, values: { firmware: "1.4.2", interval_s: 30 } });
    await Bun.sleep(250);
    await click("Refresh attributes");
    await waitForBrowser("document.body.innerText.includes('firmware') && document.body.innerText.includes('1.4.2')", Boolean, "MQTT attributes on detail view");
    await waitForBrowser("document.querySelector('.chart-host')?.getAttribute('role')", (value) => value === "img", "accessible telemetry chart");
    await setValue("#telemetry-key", "temperature", "change");
    await waitForBrowser("document.querySelector('.chart-host')?.getAttribute('aria-label')", (value) => typeof value === "string" && value.includes("Telemetry chart for temperature"), "selected telemetry key");
    await deviceMqtt.publish(`devices/${deviceId}/telemetry`, {
      event_id: `out-of-range-${timestamp}`,
      ts: timestamp - 1_500,
      values: { temperature: 99 },
    });
    expect((await deviceMqtt.waitPublish(`devices/${deviceId}/telemetry/ack`)).status).toBe("stored");
    await waitForBrowser(
      "document.querySelector('.chart-host')?.getAttribute('aria-label')",
      (value) => value === "Telemetry chart for temperature, 1 numeric sample",
      "real MQTT event outside the selected chart range",
    );
    const chartNode = await call("DOM.getDocument");
    const chartRoot = (chartNode.result?.root as { nodeId?: number } | undefined)?.nodeId;
    if (chartRoot === undefined) throw new Error("Could not inspect the browser accessibility tree.");
    const chartQuery = await call("DOM.querySelector", { nodeId: chartRoot, selector: ".chart-host" });
    const chartNodeId = (chartQuery.result?.nodeId as number | undefined);
    if (!chartNodeId) throw new Error("Telemetry chart was not present in the browser DOM.");
    const described = await call("DOM.describeNode", { nodeId: chartNodeId });
    const backendNodeId = (described.result?.node as { backendNodeId?: number } | undefined)?.backendNodeId;
    const axTree = await call("Accessibility.getPartialAXTree", { backendNodeId });
    const axNodes = (axTree.result?.nodes as Array<{ role?: { value?: string }; name?: { value?: string } }> | undefined) ?? [];
    if (!axNodes.some((node) => node.role?.value === "image" && node.name?.value?.includes("Telemetry chart for"))) {
      throw new Error(`Chart accessibility nodes: ${JSON.stringify(axNodes)}`);
    }
    await call("Emulation.setDeviceMetricsOverride", { width: 320, height: 900, deviceScaleFactor: 1, mobile: false });
    await Bun.sleep(150);
    const narrowPage = await evaluate("({ viewport: window.innerWidth, content: document.documentElement.scrollWidth })") as { viewport: number; content: number };
    if (narrowPage.content > narrowPage.viewport + 1) {
      const overflow = await evaluate(`(() => {
        const chart = document.querySelector('.uplot');
        const ancestors = [];
        for (let element = chart; element && ancestors.length < 8; element = element.parentElement) {
          const rect = element.getBoundingClientRect();
          const style = getComputedStyle(element);
          ancestors.push({ tag: element.tagName, id: element.id, className: typeof element.className === 'string' ? element.className : '', width: Math.round(rect.width), minWidth: style.minWidth, maxWidth: style.maxWidth, overflowX: style.overflowX });
        }
        return ancestors;
      })()`);
      const offenders = await evaluate(`[...document.querySelectorAll('body *')].map((element) => {
        const rect = element.getBoundingClientRect();
        return { tag: element.tagName, id: element.id, className: typeof element.className === 'string' ? element.className : '', left: Math.round(rect.left), right: Math.round(rect.right), width: Math.round(rect.width), scrollWidth: element.scrollWidth };
      }).filter((item) => item.width && (item.left < 0 || item.right > window.innerWidth + 1)).sort((a, b) => b.right - a.right).slice(0, 12)`);
      throw new Error(`320px viewport overflows: ${JSON.stringify({ narrowPage, ancestors: overflow, offenders })}`);
    }
    await call("Emulation.setDeviceMetricsOverride", { width: 1280, height: 900, deviceScaleFactor: 1, mobile: false });

    await setValue("#edit-device-name", `${deviceName}-edited`);
    await setValue("#edit-device-metadata", JSON.stringify({ room: "lab", owner: "ops" }));
    await evaluate("document.querySelector('#edit-device-name')?.closest('form')?.requestSubmit()");
    await waitForBrowser("document.querySelector('#device-summary-title')?.textContent?.trim()", (value) => value === `${deviceName}-edited`, "device edit");
    await waitForBrowser("document.querySelector('#edit-device-metadata')?.value.includes('owner')", Boolean, "edited metadata");

    await setValue("#command-params", "[");
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Create command'", "Create command");
    await enter();
    await waitForBrowser("document.querySelector('[role=alert]')?.textContent", (value) => typeof value === "string" && value.includes("Command parameters must be valid JSON"), "command validation error");
    await setValue("#command-params", JSON.stringify({ delay_s: 5 }));
    await setValue("#command-name", "reboot");
    await setValue("#command-ttl", "60000");
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Create command'", "Create command");
    await enter();
    const command = await deviceMqtt.waitPublish(`devices/${deviceId}/commands`);
    expect(command.name).toBe("reboot");
    await deviceMqtt.publish(`devices/${deviceId}/commands/ack`, {
      command_id: command.command_id,
      status: "succeeded",
      result: { uptime_s: 42 },
      error: null,
    });
    await waitForBrowser("document.body.innerText.includes('succeeded')", Boolean, "command application ACK");

    await setValue("#command-ttl", "1000");
    await click("Create command");
    const expiringCommand = await deviceMqtt.waitPublish(`devices/${deviceId}/commands`);
    expect(expiringCommand.command_id).toBeString();
    await waitForBrowser("document.body.innerText.includes('expired')", Boolean, "command expiry state", 12_000);

    await evaluate("window.confirm = () => true");
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Rotate token'", "Rotate token");
    await enter();
    await waitForBrowser("document.querySelector('.token-dialog[open]') !== null", Boolean, "rotated device token");
    await auditKeyboard("rotated token dialog");
    const rotatedToken = await evaluate("document.querySelector('#issued-token')?.value") as string;
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Close and clear token'", "rotated token dialog close");
    await enter();
    await Bun.sleep(50);
    const restoredFocus = await evaluate(`(() => {
      const active = document.activeElement;
      return { tag: active?.tagName, id: active instanceof HTMLElement ? active.id : "", text: active?.textContent?.trim() };
    })()`);
    if ((restoredFocus as { text?: string }).text !== "Rotate token") {
      throw new Error(`Token dialog did not restore focus to its trigger: ${JSON.stringify(restoredFocus)}`);
    }
    await waitForBrowser("[...document.querySelectorAll('button')].some((button) => button.textContent?.trim() === 'Disable device')", Boolean, "device refreshed after credential rotation");
    await waitFor(() => deviceMqtt.closedConnection.then(() => true), "old MQTT session kicked", 10_000);
    const denied = await MqttDevice.connect(mqttHelper, "127.0.0.1", mqttPort, deviceId, initialToken);
    expect(denied.code).toBe(4);
    const rotated = await MqttDevice.connect(mqttHelper, "127.0.0.1", mqttPort, deviceId, rotatedToken);
    if (rotated.code !== 0 || !rotated.client) throw new Error(`Rotated MQTT credential failed with code ${rotated.code}.`);
    let activeMqtt = rotated.client;
    rotatedMqtt = activeMqtt;
    await activeMqtt.subscribe(`devices/${deviceId}/telemetry/ack`);
    await activeMqtt.subscribe(`devices/${deviceId}/commands`);

    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Devices'", "Devices navigation");
    await enter();
    await waitForBrowser("document.querySelector('#devices-title') !== null", Boolean, "device list before disable");
    await waitForBrowser("document.activeElement?.id === 'devices-title'", Boolean, "focus moved to Devices after re-navigation");
    await auditKeyboard("device list after reauthentication");
    expect(await evaluate("document.querySelector('.chart-host') === null")).toBe(true);
    await waitForBrowser(`document.querySelector('.table-wrap tbody')?.innerText.includes(${JSON.stringify(`${deviceName}-edited`)})`, Boolean, "edited device row before disable");
    await tabToControl(`active instanceof HTMLButtonElement && active.textContent?.trim() === 'Disable' && active.closest('tr')?.innerText.includes(${JSON.stringify(`${deviceName}-edited`)})`, "disable device");
    await enter();
    await waitFor(() => requestMethods.includes(`PATCH /api/v1/devices/${deviceId}`), "disable-device PATCH request");
    await waitForBrowser(`(() => {
      const row = [...document.querySelectorAll('.table-wrap tbody tr')].find((item) => item.innerText.includes(${JSON.stringify(`${deviceName}-edited`)}));
      return [...(row?.querySelectorAll('button') ?? [])].some((button) => button.textContent?.trim() === 'Enable');
    })()`, Boolean, "device disable");
    await waitFor(() => activeMqtt.closedConnection.then(() => true), "disabled MQTT session kicked", 10_000);
    const disabledToken = await MqttDevice.connect(mqttHelper, "127.0.0.1", mqttPort, deviceId, rotatedToken);
    expect(disabledToken.code).toBe(4);
    await tabToControl(`active instanceof HTMLButtonElement && active.textContent?.trim() === 'Enable' && active.closest('tr')?.innerText.includes(${JSON.stringify(`${deviceName}-edited`)})`, "enable device");
    await enter();
    await waitForBrowser(`(() => {
      const row = [...document.querySelectorAll('.table-wrap tbody tr')].find((item) => item.innerText.includes(${JSON.stringify(`${deviceName}-edited`)}));
      return [...(row?.querySelectorAll('button') ?? [])].some((button) => button.textContent?.trim() === 'Disable');
    })()`, Boolean, "device enable");
    const reconnected = await MqttDevice.connect(mqttHelper, "127.0.0.1", mqttPort, deviceId, rotatedToken);
    if (reconnected.code !== 0 || !reconnected.client) throw new Error(`Re-enabled MQTT credential failed with code ${reconnected.code}.`);
    await activeMqtt.close();
    activeMqtt = reconnected.client;
    rotatedMqtt = activeMqtt;
    await activeMqtt.subscribe(`devices/${deviceId}/telemetry/ack`);
    await activeMqtt.subscribe(`devices/${deviceId}/commands`);
    await waitForBrowser(`document.querySelector('.table-wrap tbody')?.innerText.includes(${JSON.stringify(`${deviceName}-edited`)})`, Boolean, "enabled device row after reconnect");
    await tabToControl(`active instanceof HTMLButtonElement && active.textContent?.trim() === ${JSON.stringify(`${deviceName}-edited`)}`, "reopen edited device");
    await enter();
    await waitForBrowser("document.querySelector('#device-title') !== null", Boolean, "reopened device after enable");
    await waitForBrowser("document.activeElement?.id === 'device-title'", Boolean, "focus moved to reopened device detail");
    await auditKeyboard("device detail after re-enable");
    await waitForBrowser(`[...document.querySelectorAll('#telemetry-key option')].some((option) => option.value === "temperature")`, Boolean, "numeric telemetry key option");
    await setValue("#telemetry-key", "temperature", "change");
    await waitForBrowser("document.querySelector('#telemetry-key')?.value", (value) => value === "temperature", "selected temperature key");

    const historyReadsBeforeRangeUpdate = requestCount(requestUrls, `/api/v1/devices/${deviceId}/telemetry`);
    await setValue("#history-from", historyFromInput);
    const reconnectHistoryToInput = await evaluate(`(() => {
      const date = new Date(Date.now());
      const local = new Date(date.getTime() - date.getTimezoneOffset() * 60_000);
      return local.toISOString().slice(0, 23);
    })()`) as string;
    await setValue("#history-to", reconnectHistoryToInput);
    const reconnectHistoryFrom = await evaluate("new Date(document.querySelector('#history-from')?.value ?? '').getTime()") as number;
    const reconnectHistoryTo = await evaluate("new Date(document.querySelector('#history-to')?.value ?? '').getTime()") as number;
    await waitFor(() => requestUrls.some((url) => {
      const request = new URL(url);
      return request.pathname === `/api/v1/devices/${deviceId}/telemetry`
        && request.searchParams.get("from") === String(reconnectHistoryFrom)
        && request.searchParams.get("to") === String(reconnectHistoryTo);
    }), "updated telemetry range request");
    expect(requestCount(requestUrls, `/api/v1/devices/${deviceId}/telemetry`)).toBeGreaterThan(historyReadsBeforeRangeUpdate);
    const beforeDetailReconnect = requestCounts(requestUrls);
    const beforeCommandDetailReconnect = requestCountPrefix(requestUrls, "/api/v1/commands/");
    await call("Network.emulateNetworkConditions", { offline: true, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
    await evaluate("window.__iotSockets.at(-1)?.close(4000, 'detail reconnect')");
    await waitForBrowser("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates reconnecting", "detail WebSocket disconnect");
    const missedAt = reconnectHistoryFrom + 100;
    await activeMqtt.publish(`devices/${deviceId}/telemetry`, {
      event_id: `reconnect-${missedAt}`,
      ts: missedAt,
      values: { temperature: 25 },
    });
    expect((await activeMqtt.waitPublish(`devices/${deviceId}/telemetry/ack`)).status).toBe("stored");
    await call("Network.emulateNetworkConditions", { offline: false, latency: 0, downloadThroughput: -1, uploadThroughput: -1 });
    await waitForBrowser("document.querySelector('.connection')?.textContent?.trim()", (value) => value === "Live updates connected", "detail WebSocket reconnect");
    await waitForBrowser("document.body.innerText.includes('Backfill scanned') && document.body.innerText.includes('committed checkpoint')", Boolean, "commit-order REST telemetry backfill");
    expect(requestCount(requestUrls, `/api/v1/devices/${deviceId}`)).toBeGreaterThan(beforeDetailReconnect.device);
    expect(requestCount(requestUrls, `/api/v1/devices/${deviceId}/attributes`)).toBeGreaterThan(beforeDetailReconnect.attributes);
    expect(requestCount(requestUrls, `/api/v1/devices/${deviceId}/telemetry/latest`)).toBeGreaterThan(beforeDetailReconnect.latest);
    expect(requestCount(requestUrls, `/api/v1/devices/${deviceId}/telemetry`)).toBeGreaterThan(beforeDetailReconnect.history);
    expect(requestCount(requestUrls, `/api/v1/devices/${deviceId}/commands`)).toBeGreaterThan(beforeDetailReconnect.commands);
    expect(requestCountPrefix(requestUrls, "/api/v1/commands/")).toBeGreaterThan(beforeCommandDetailReconnect);
    expect(requestUrls.some((url) => new URL(url).searchParams.has("after_id"))).toBe(true);
    expect(await evaluate("document.querySelector('#telemetry-key')?.value")).toBe("temperature");
    expect(await evaluate("document.querySelector('.chart-host')?.getAttribute('aria-label')")).toBe("Telemetry chart for temperature, 2 numeric samples");

    const beforeOverviewReconnect = requestCount(requestUrls, "/api/v1/overview");
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Overview'", "Overview navigation");
    await enter();
    await waitForBrowser("document.querySelector('#overview-title') !== null", Boolean, "overview view");
    await waitForBrowser("document.activeElement?.id === 'overview-title'", Boolean, "focus moved to overview");
    await auditKeyboard("overview after reconnect");
    await waitForBrowser("window.__iotOverviewIntervals.size === 1", Boolean, "overview interval restarted");
    await waitForBrowser("['Devices online', 'Telemetry events · 24h', 'Commands in progress', 'Ingest queue'].every((label) => document.body.innerText.includes(label))", Boolean, "overview API metrics");
    await reconnectOffline();
    await waitFor(() => requestCount(requestUrls, "/api/v1/overview") > beforeOverviewReconnect, "overview REST refetch");

    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Config'", "Config navigation");
    await enter();
    await waitForBrowser("document.querySelector('#config-title') !== null", Boolean, "runtime configuration view");
    await waitForBrowser("document.activeElement?.id === 'config-title'", Boolean, "focus moved to runtime configuration");
    await auditKeyboard("runtime configuration");
    await waitForBrowser("window.__iotOverviewIntervals.size === 0", Boolean, "overview interval cleanup on config");
    await waitForBrowser("document.querySelector('#logging-level') !== null", Boolean, "runtime settings loaded");
    expect(await evaluate(`([...document.querySelectorAll('input,select,textarea')].every((field) => field.labels?.length || field.getAttribute('aria-label')) && [...document.querySelectorAll('button')].every((button) => button.getAttribute('aria-label') || button.textContent?.trim()))`)).toBe(true);
    await setValue("#logging-level", "debug", "change");
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Save runtime settings'", "Save runtime settings");
    await enter();
    await waitForBrowser("document.body.innerText.includes('Current revision: 1')", Boolean, "runtime config PATCH");
    const beforeConfigReconnect = requestCount(requestUrls, "/api/v1/config");
    await reconnectOffline();
    await waitFor(() => requestCount(requestUrls, "/api/v1/config") > beforeConfigReconnect, "config REST refetch");
    expect(await evaluate("document.querySelector('#logging-level')?.value")).toBe("debug");

    const database = new Database(dbPath);
    database.exec("CREATE TRIGGER fail_ui_telemetry BEFORE INSERT ON telemetry_events BEGIN SELECT RAISE(ABORT, 'test storage outage'); END");
    database.close();
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Overview'", "Overview navigation");
    await enter();
    await waitForBrowser("document.querySelector('#overview-title') !== null", Boolean, "overview before storage failure");
    await activeMqtt.publish(`devices/${deviceId}/telemetry`, {
      event_id: `storage-${Date.now()}`,
      ts: Date.now(),
      values: { temperature: 26 },
    });
    await waitForBrowser("document.body.innerText.includes('Storage is unavailable')", Boolean, "real storage-unavailable warning");
    await waitForBrowser("document.querySelector('.notice-error[role=alert]')?.textContent?.includes('New telemetry may not be saved')", Boolean, "overview storage-unavailable alert");
    const cleanupDatabase = new Database(dbPath);
    cleanupDatabase.exec("DROP TRIGGER fail_ui_telemetry");
    cleanupDatabase.close();

    const cookieReply = await call("Network.getAllCookies");
    const cookies = (cookieReply.result?.cookies as Array<{ name: string; value: string }> | undefined) ?? [];
    const sessionToken = cookies.find((cookie) => cookie.name === "iot_session")?.value;
    if (!sessionToken) throw new Error("Could not find the browser's HttpOnly admin session cookie.");
    const sessionHash = createHash("sha256").update(sessionToken).digest();
    const sessionDatabase = new Database(dbPath);
    const expired = sessionDatabase.query("UPDATE sessions SET expires_at = ? WHERE token_hash = ?").run(Date.now() - 1_000, sessionHash);
    sessionDatabase.close();
    expect(expired.changes).toBe(1);
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Devices'", "Devices navigation after session expiry");
    await enter();
    await waitForBrowser("document.querySelector('form[aria-label=\"Admin login\"]') !== null", Boolean, "expired-session login screen");
    await auditKeyboard("expired-session login");
    expect(await evaluate("document.body.innerText.includes('Your session has expired. Sign in again.')")).toBe(true);
    expect(await evaluate("document.querySelector('.chart-host') === null")).toBe(true);
    expect(await evaluate("localStorage.length")).toBe(0);

    await setValue("input[name='username']", "ui-admin");
    await setValue("input[name='password']", password);
    await tabToControl("active.matches('form[aria-label=\\\"Admin login\\\"] button[type=\\\"submit\\\"]')", "re-authentication submit");
    await enter();
    await waitForBrowser("document.querySelector('#overview-title') !== null", Boolean, "login after expiry");
    await waitForBrowser("document.activeElement?.id === 'overview-title'", Boolean, "focus moved to overview after re-authentication");
    await auditKeyboard("overview after re-authentication");
    await tabToControl("active instanceof HTMLButtonElement && active.textContent?.trim() === 'Log out'", "Log out");
    await enter();
    await waitForBrowser("document.querySelector('form[aria-label=\"Admin login\"]') !== null", Boolean, "server-confirmed logout");
    expect(requestMethods).toContain("POST /api/v1/auth/logout");
    await waitFor(async () => await evaluate("window.__iotSockets.at(-1)?.readyState") === 3, "logout WebSocket cleanup");
    await waitForBrowser("window.__iotStableTimers.size === 0 && window.__iotOverviewIntervals.size === 0", Boolean, "logout timer cleanup");
  } finally {
    devtools?.close();
    if (chrome) {
      chrome.kill();
      await chrome.exited;
    }
    await mqtt?.close();
    await rotatedMqtt?.close();
    if (vite) await vite.close();
    if (server) {
      server.kill("SIGTERM");
      const finished = await Promise.race([server.exited.then(() => true), Bun.sleep(5_000).then(() => false)]);
      if (!finished) {
        server.kill("SIGKILL");
        await server.exited;
      }
    }
    await rm(temp, { recursive: true, force: true });
  }
}, 180_000);

async function freePort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>((resolveListen, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", resolveListen);
  });
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Could not reserve a local test port.");
  const port = address.port;
  await new Promise<void>((resolveClose, reject) => server.close((error) => error ? reject(error) : resolveClose()));
  return port;
}

async function waitFor(check: () => boolean | Promise<boolean>, label: string, timeoutMs = 10_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await check()) return;
    await Bun.sleep(50);
  }
  throw new Error(`Timed out waiting for ${label}.`);
}

function requestCounts(urls: string[]): { device: number; attributes: number; latest: number; history: number; commands: number } {
  const deviceId = urls.map((url) => new URL(url).pathname.match(/\/devices\/([^/]+)/)?.[1]).find(Boolean) ?? "";
  return {
    device: requestCount(urls, `/api/v1/devices/${deviceId}`),
    attributes: requestCount(urls, `/api/v1/devices/${deviceId}/attributes`),
    latest: requestCount(urls, `/api/v1/devices/${deviceId}/telemetry/latest`),
    history: requestCount(urls, `/api/v1/devices/${deviceId}/telemetry`),
    commands: requestCount(urls, `/api/v1/devices/${deviceId}/commands`),
  };
}

function requestCount(urls: string[], path: string): number {
  return urls.filter((url) => {
    try {
      return new URL(url).pathname === path;
    } catch {
      return false;
    }
  }).length;
}

function requestCountPrefix(urls: string[], prefix: string): number {
  return urls.filter((url) => {
    try {
      return new URL(url).pathname.startsWith(prefix);
    } catch {
      return false;
    }
  }).length;
}
