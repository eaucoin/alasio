/**
 * A Docker Engine in memory, serving the Engine API over a Unix socket as Docker does,
 * for the tests of the client (../../src/cluster/docker.ts) and of what drives it. It
 * keeps images, networks, volumes and containers, records every request, upgrades an
 * exec's start to a raw stream as Docker does, and answers commands run in containers
 * and files read from them as the test says.
 */
import { once } from "node:events";
import { mkdtempSync, rmSync } from "node:fs";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import type { Duplex } from "node:stream";
import { buffer } from "node:stream/consumers";
import { join } from "node:path";

/** A request the fake was sent: its method, its path without the API version, its query, and its JSON body. */
export interface DockerRequest {
  readonly method: string;
  readonly path: string;
  readonly query: URLSearchParams;
  readonly body: unknown;
}

/** What a command run in a container came to. */
export interface FakeExecResult {
  readonly exitCode: number;
  readonly stdout?: string;
  readonly stderr?: string;
}

/** How the test answers a command run in `container`, given what it was sent on its stdin. */
export type ExecAnswer = (container: string, command: readonly string[], stdin: Buffer) => FakeExecResult;

/** A container the fake keeps: the body it was created with, its state, and when it last started (zero time before it has). */
export interface FakeContainer {
  readonly body: { readonly Labels?: Readonly<Record<string, string>> } & Record<string, unknown>;
  state: "created" | "running" | "exited";
  startedAt?: string;
}

/** Docker's StartedAt of a container that never started. */
const NEVER_STARTED = "0001-01-01T00:00:00Z";

export interface FakeDockerOptions {
  readonly onExec?: ExecAnswer;
  /** The content of the file at `path` in `container`, or null when there is none. */
  readonly files?: (container: string, path: string) => Buffer | null;
  /** Whether a pull of `reference` succeeds; one that does not fails as Docker's do, after its 200. */
  readonly pullable?: (reference: string) => boolean;
}

export interface FakeDocker {
  /** What `DOCKER_HOST` says to reach it. */
  readonly host: string;
  readonly requests: DockerRequest[];
  readonly images: Set<string>;
  readonly networks: Map<string, { readonly Name: string; readonly Labels: Readonly<Record<string, string>>; readonly Subnet: string }>;
  readonly volumes: Map<string, Readonly<Record<string, string>>>;
  readonly containers: Map<string, FakeContainer>;
  /** The requests that changed an image, network, volume or container, as `METHOD path`, in order. */
  readonly changes: () => readonly string[];
  readonly close: () => Promise<void>;
}

/** A tar archive of one file, `name`, holding `content`, as Docker's archive endpoint sends one. */
export function tarOf(name: string, content: Buffer): Buffer {
  const header = Buffer.alloc(512);
  header.write(name, 0, 100, "utf8");
  header.write("0000644\0", 100);
  header.write("0000000\0", 108);
  header.write("0000000\0", 116);
  header.write(`${content.length.toString(8).padStart(11, "0")}\0`, 124);
  header.write("00000000000\0", 136);
  header.write("0", 156);
  header.write("ustar\0", 257);
  header.write("00", 263);
  // The checksum is the sum of the header's bytes with its own field as spaces.
  header.write("        ", 148);
  const checksum = header.reduce((sum, byte) => sum + byte, 0);
  header.write(`${checksum.toString(8).padStart(6, "0")}\0 `, 148);
  const padding = Buffer.alloc((512 - (content.length % 512)) % 512);
  return Buffer.concat([header, content, padding, Buffer.alloc(1024)]);
}

/** Output as Docker frames it for an exec without a terminal: per stream, an 8-byte header and the bytes. */
export function frame(stream: 1 | 2, content: string): Buffer {
  const payload = Buffer.from(content);
  const header = Buffer.alloc(8);
  header[0] = stream;
  header.writeUInt32BE(payload.length, 4);
  return Buffer.concat([header, payload]);
}

const json = (response: ServerResponse, status: number, body: unknown): void => {
  response.writeHead(status, { "Content-Type": "application/json" });
  response.end(JSON.stringify(body));
};

const refuse = (response: ServerResponse, status: number, message: string): void => json(response, status, { message });

/** Whether `labels` carry every `key=value` of a list call's label filter. */
function matches(labels: Readonly<Record<string, string>> | undefined, filters: string | null): boolean {
  const wanted: readonly string[] = filters ? (JSON.parse(filters) as { label?: string[] }).label ?? [] : [];
  return wanted.every((pair) => {
    const [key = "", value = ""] = pair.split("=");
    return labels?.[key] === value;
  });
}

/** Serves a fake Docker Engine on a Unix socket in a directory of its own, until `close`. */
export async function serveFakeDocker({
  onExec = () => ({ exitCode: 0 }),
  files = () => null,
  pullable = () => true,
}: FakeDockerOptions = {}): Promise<FakeDocker> {
  // Under /tmp, not TMPDIR, which may be too long a path for a socket's (108 bytes at most).
  const directory = mkdtempSync("/tmp/fake-docker-");
  const socket = join(directory, "docker.sock");
  const requests: DockerRequest[] = [];
  const images = new Set<string>();
  const networks: FakeDocker["networks"] = new Map();
  const volumes: FakeDocker["volumes"] = new Map();
  const containers = new Map<string, FakeContainer>();
  const execs = new Map<string, { readonly container: string; readonly command: readonly string[]; readonly stdin: boolean; exitCode: number | null }>();

  const record = (request: IncomingMessage, body: Buffer): DockerRequest => {
    const url = new URL(request.url ?? "/", "http://docker");
    const recorded = {
      method: request.method ?? "GET",
      path: decodeURIComponent(url.pathname.replace(/^\/v[\d.]+/u, "")),
      query: url.searchParams,
      body: body.length ? JSON.parse(body.toString("utf8")) : undefined,
    };
    requests.push(recorded);
    return recorded;
  };

  const handle = (request: DockerRequest, response: ServerResponse): void => {
    const { method, path, query, body } = request;
    const route = `${method} ${path}`;
    let match: RegExpExecArray | null;
    if (route === "GET /version") return json(response, 200, { Version: "29.0.0", ApiVersion: "1.52" });
    if ((match = /^GET \/images\/(.+)\/json$/u.exec(route))) {
      const reference = match[1] ?? "";
      return images.has(reference) ? json(response, 200, { Id: `sha256:${reference}` }) : refuse(response, 404, `No such image: ${reference}`);
    }
    if (route === "POST /images/create") {
      const reference = query.get("fromImage") ?? "";
      response.writeHead(200, { "Content-Type": "application/json" });
      response.write(`${JSON.stringify({ status: `Pulling from ${reference}` })}\n`);
      if (!pullable(reference)) return void response.end(`${JSON.stringify({ errorDetail: { message: "denied" }, error: "denied" })}\n`);
      images.add(reference);
      return void response.end(`${JSON.stringify({ status: `Downloaded newer image for ${reference}` })}\n`);
    }
    if (route === "GET /images/get") {
      response.writeHead(200, { "Content-Type": "application/x-tar" });
      return void response.end(`archive of ${query.getAll("names").join(" ")}`);
    }
    if (route === "POST /networks/create") {
      const network = body as { Name: string; Labels: Record<string, string>; IPAM?: { Config: [{ Subnet: string }] } };
      if (networks.has(network.Name)) return refuse(response, 409, `network with name ${network.Name} already exists`);
      networks.set(network.Name, { Name: network.Name, Labels: network.Labels, Subnet: network.IPAM?.Config[0].Subnet ?? "172.18.0.0/16" });
      return json(response, 201, { Id: network.Name });
    }
    if (route === "GET /networks") {
      return json(response, 200, [...networks.values()].map((network) => ({ Name: network.Name, Labels: network.Labels, IPAM: { Config: [{ Subnet: network.Subnet }] } })));
    }
    if ((match = /^(GET|DELETE) \/networks\/([^/]+)$/u.exec(route))) {
      const network = networks.get(match[2] ?? "");
      if (!network) return refuse(response, 404, `network ${match[2]} not found`);
      if (match[1] === "DELETE") {
        networks.delete(network.Name);
        return void response.writeHead(204).end();
      }
      return json(response, 200, { Name: network.Name, Labels: network.Labels, IPAM: { Config: [{ Subnet: network.Subnet }] } });
    }
    if (route === "POST /volumes/create") {
      const volume = body as { Name: string; Labels: Record<string, string> };
      if (!volumes.has(volume.Name)) volumes.set(volume.Name, volume.Labels);
      return json(response, 201, { Name: volume.Name, Labels: volumes.get(volume.Name) });
    }
    if (route === "GET /volumes") {
      const found = [...volumes].filter(([, labels]) => matches(labels, query.get("filters")));
      return json(response, 200, { Volumes: found.map(([Name, Labels]) => ({ Name, Labels })) });
    }
    if ((match = /^DELETE \/volumes\/([^/]+)$/u.exec(route))) {
      if (!volumes.delete(match[1] ?? "")) return refuse(response, 404, `get ${match[1]}: no such volume`);
      return void response.writeHead(204).end();
    }
    if (route === "GET /containers/json") {
      const found = [...containers].filter(([, container]) => matches(container.body.Labels, query.get("filters")));
      return json(response, 200, found.map(([name, container]) => ({ Names: [`/${name}`], Labels: container.body.Labels ?? {}, State: container.state })));
    }
    if (route === "POST /containers/create") {
      const name = query.get("name") ?? "";
      if (containers.has(name)) return refuse(response, 409, `Conflict. The container name "/${name}" is already in use`);
      containers.set(name, { body: body as FakeContainer["body"], state: "created" });
      return json(response, 201, { Id: name, Warnings: [] });
    }
    if ((match = /^(GET|POST|DELETE) \/containers\/([^/]+)(?:\/(json|start|stop|exec|archive))?$/u.exec(route))) {
      const [, verb, name = "", action] = match;
      const container = containers.get(name);
      if (!container) return refuse(response, 404, `No such container: ${name}`);
      if (verb === "GET" && action === "json") {
        return json(response, 200, {
          Name: `/${name}`,
          Config: { Labels: container.body.Labels ?? null },
          State: { Status: container.state, Running: container.state === "running", StartedAt: container.startedAt ?? NEVER_STARTED },
        });
      }
      if (verb === "DELETE") {
        containers.delete(name);
        return void response.writeHead(204).end();
      }
      if (action === "start" || action === "stop") {
        const state = action === "start" ? "running" : "exited";
        if (container.state === state) return void response.writeHead(304).end();
        container.state = state;
        if (state === "running") container.startedAt = new Date().toISOString();
        return void response.writeHead(204).end();
      }
      if (action === "exec") {
        if (container.state !== "running") return refuse(response, 409, `container ${name} is not running`);
        const exec = body as { Cmd: string[]; AttachStdin: boolean };
        const id = `exec-${execs.size}`;
        execs.set(id, { container: name, command: exec.Cmd, stdin: exec.AttachStdin, exitCode: null });
        return json(response, 201, { Id: id });
      }
      if (action === "archive") {
        const content = files(name, query.get("path") ?? "");
        if (!content) return refuse(response, 404, `Could not find the file ${query.get("path")} in container ${name}`);
        response.writeHead(200, { "Content-Type": "application/x-tar" });
        return void response.end(tarOf((query.get("path") ?? "").split("/").at(-1) ?? "", content));
      }
    }
    if ((match = /^GET \/exec\/([^/]+)\/json$/u.exec(route))) {
      const exec = execs.get(match[1] ?? "");
      return exec ? json(response, 200, { Running: exec.exitCode === null, ExitCode: exec.exitCode }) : refuse(response, 404, "No such exec instance");
    }
    refuse(response, 404, `page not found: ${route}`);
  };

  /** An exec's start, upgraded to a raw stream: its body first, then what the client writes to the command's stdin until it closes its side. */
  const attach = async (request: IncomingMessage, socket: Duplex, head: Buffer): Promise<void> => {
    const chunks = [head];
    socket.on("data", (chunk: Buffer) => chunks.push(chunk));
    const length = Number(request.headers["content-length"] ?? 0);
    const id = /\/exec\/([^/]+)\/start$/u.exec(request.url ?? "")?.[1] ?? "";
    const exec = execs.get(id);
    if (!exec) {
      socket.end("HTTP/1.1 404 Not Found\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}");
      return;
    }
    socket.write("HTTP/1.1 101 UPGRADED\r\nContent-Type: application/vnd.docker.raw-stream\r\nConnection: Upgrade\r\nUpgrade: tcp\r\n\r\n");
    // A client that gives up on the exec resets the connection; nothing is answered then.
    socket.on("error", () => socket.destroy());
    if (exec.stdin) {
      const closed = await new Promise<boolean>((resolve) => {
        socket.once("end", () => resolve(false));
        socket.once("close", () => resolve(true));
      });
      if (closed) return;
    }
    const received = Buffer.concat(chunks);
    record(request, received.subarray(0, length));
    const { exitCode, stdout = "", stderr = "" } = onExec(exec.container, exec.command, received.subarray(length));
    exec.exitCode = exitCode;
    socket.end(Buffer.concat([...(stdout ? [frame(1, stdout)] : []), ...(stderr ? [frame(2, stderr)] : [])]));
  };

  const server = createServer((request, response) => {
    buffer(request).then((body) => handle(record(request, body), response), (error: unknown) => response.destroy(error as Error));
  });
  server.on("upgrade", (request: IncomingMessage, socket: Duplex, head: Buffer) => void attach(request, socket, head));
  server.listen(socket);
  await once(server, "listening");

  return {
    host: `unix://${socket}`,
    requests,
    images,
    networks,
    volumes,
    containers,
    changes: () => requests.filter(({ method, path }) => method !== "GET" && !/\/exec(\/|$)/u.test(path)).map(({ method, path }) => `${method} ${path}`),
    close: async () => {
      server.closeAllConnections();
      server.close();
      await once(server, "close");
      rmSync(directory, { recursive: true, force: true });
    },
  };
}
