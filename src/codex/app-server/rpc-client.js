import { elapsedMs, startAppServerProcess } from "./process.js";
import { serverRequestResponse } from "./protocol.js";

const REQUEST_TIMEOUT_MS = 5 * 60 * 1000;
const START_TIMEOUT_MS = 20 * 1000;

export class AppServerRpcClient {
  constructor({ log, onNotification, onFailure }) {
    this.log = log;
    this.onNotification = onNotification;
    this.onFailure = onFailure;
    this.process = null;
    this.nextId = 1;
    this.pending = new Map();
    this.initialized = false;
    this.startPromise = null;
  }

  async start({ env, cwd }) {
    if (this.process?.child && !this.process.child.killed && this.initialized) {
      return;
    }
    if (this.startPromise) {
      return this.startPromise;
    }
    this.startPromise = this.startInner({ env, cwd }).finally(() => {
      this.startPromise = null;
    });
    return this.startPromise;
  }

  async startInner({ env, cwd }) {
    this.stop();
    const startedAt = process.hrtime.bigint();
    // A process stopped or replaced may still report its exit, late: only
    // the current one's events count.
    const started = startAppServerProcess({
      cwd,
      env,
      onLine: (line) => this.process === started && this.handleLine(line),
      onExit: (code, signal) => this.process === started && this.handleExit(code, signal),
      onError: (error) => this.process === started && this.handleSpawnError(error),
    });
    this.process = started;
    const init = await this.request("initialize", {
      clientInfo: {
        name: "alasio_telegram",
        title: "Alasio Telegram",
        version: "1.0.0",
      },
      capabilities: {
        experimentalApi: true,
      },
    }, START_TIMEOUT_MS);
    this.notify("initialized", null);
    this.initialized = true;
    this.log.info(`initialized ms=${elapsedMs(startedAt).toFixed(1)} user_agent=${JSON.stringify(init.userAgent)}`);
  }

  stop() {
    if (this.process) {
      this.process.stop();
      this.process = null;
    }
    this.initialized = false;
    this.failPending(new Error("Codex app-server stopped"));
  }

  handleExit(code, signal) {
    this.log.warn(`app-server exited code=${code} signal=${signal}`);
    this.failPending(new Error(`Codex app-server exited code=${code} signal=${signal}`));
    this.initialized = false;
    this.process = null;
  }

  handleSpawnError(error) {
    this.log.error(`app-server spawn failed: ${error instanceof Error ? error.message : String(error)}`);
    this.failPending(error);
    this.initialized = false;
    this.process = null;
  }

  failPending(error) {
    for (const entry of this.pending.values()) {
      clearTimeout(entry.timer);
      entry.reject(error);
    }
    this.pending.clear();
    this.onFailure(error);
  }

  handleLine(line) {
    if (!line.trim()) {
      return;
    }
    let message;
    try {
      message = JSON.parse(line);
    } catch {
      this.log.warn(`Ignoring non-JSON app-server line: ${line.slice(0, 200)}`);
      return;
    }
    if (message.id != null && this.pending.has(message.id)) {
      this.resolvePending(message);
      return;
    }
    if (message.id != null && message.method) {
      this.respondToServerRequest(message);
      return;
    }
    if (message.method) {
      this.onNotification(message);
    }
  }

  resolvePending(message) {
    const entry = this.pending.get(message.id);
    this.pending.delete(message.id);
    clearTimeout(entry.timer);
    if (message.error) {
      entry.reject(new Error(message.error.message ?? JSON.stringify(message.error)));
    } else {
      entry.resolve(message.result);
    }
  }

  respondToServerRequest(message) {
    const result = serverRequestResponse(message.method);
    this.write({ id: message.id, result });
  }

  write(payload) {
    if (!this.process?.child?.stdin) {
      throw new Error("Codex app-server is not running");
    }
    this.process.child.stdin.write(`${JSON.stringify(payload)}\n`);
  }

  request(method, params, timeoutMs = REQUEST_TIMEOUT_MS) {
    const id = this.nextId;
    this.nextId += 1;
    this.write({ id, method, params });
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`Codex app-server request timed out: ${method}`));
      }, timeoutMs);
      this.pending.set(id, { resolve, reject, timer });
    });
  }

  notify(method, params) {
    this.write({ method, params });
  }
}
