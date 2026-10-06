/**
 * Docker's Engine API, over the Unix socket Docker listens on: as much of it as the local
 * cluster (./local.ts) needs, its images, network, volumes and containers and commands
 * run in them, called with node:http, so neither the Docker CLI nor a client library is
 * needed. Paths are those of API version 1.44 (Docker 25), the oldest it is written for.
 *
 * The socket is the one `DOCKER_HOST` names (`unix://PATH`), /var/run/docker.sock when
 * it names none.
 */
import { type ClientRequest, type IncomingMessage, request as httpRequest, STATUS_CODES } from "node:http";
import type { Duplex, Readable } from "node:stream";
import { buffer } from "node:stream/consumers";

import { Context, Effect, Layer, Result, Schedule, Schema, type Scope } from "effect";

const API_VERSION = "v1.44";
const DEFAULT_SOCKET = "/var/run/docker.sock";

/** Docker refused a call, or could not be reached: the status it answered with, when it answered. */
export class DockerError extends Schema.TaggedError<DockerError>()("DockerError", {
  /** The call, as its method and path. */
  call: Schema.String,
  /** Docker's HTTP status (404 for absent, 409 for a conflict), when it answered. */
  status: Schema.optional(Schema.Number),
  /** What Docker answered, or why it could not be reached. */
  reason: Schema.String,
}) {
  override get message(): string {
    return `Docker ${this.status === undefined ? "failed" : `answered ${this.status} to`} ${this.call}: ${this.reason}`;
  }
}

/** `DOCKER_HOST` names a Docker that is not reached over a Unix socket. */
export class DockerHostUnsupported extends Schema.TaggedError<DockerHostUnsupported>()("DockerHostUnsupported", {
  host: Schema.String,
}) {
  override get message(): string {
    return `DOCKER_HOST is ${this.host}, but alasio reaches Docker only over a Unix socket (unix://PATH)`;
  }
}

/** Whether `error` is Docker's answer `status`. */
export const hasStatus = (status: number) => (error: DockerError): boolean => error.status === status;

/**
 * Whether `error` is Docker's answer to removing a container it killed but did not see
 * exit within the time it waits: the kill stands, and the container exits once its
 * processes do, which those waiting on a FUSE mount whose client dies with them, as
 * JuiceFS's on a node are, can take longer to.
 */
const killedNotYetExited = (error: DockerError): boolean => error.status === 500 && error.reason.includes("did not receive an exit event");

/** The socket of the Docker `env` names, by `DOCKER_HOST`. */
export function dockerSocket(env: Readonly<NodeJS.ProcessEnv>): Result.Result<string, DockerHostUnsupported> {
  const host = env["DOCKER_HOST"];
  if (!host) return Result.succeed(DEFAULT_SOCKET);
  if (host.startsWith("unix://")) return Result.succeed(host.slice("unix://".length));
  return Result.fail(new DockerHostUnsupported({ host }));
}

/** What `version` reads of Docker's version. */
export interface DockerVersion {
  readonly Version: string;
  readonly ApiVersion: string;
}

/** What is read of an image: that it is there. */
export interface ImageInspect {
  readonly Id: string;
}

/** What is read of a network. */
export interface NetworkInspect {
  readonly Name: string;
  readonly Labels: Readonly<Record<string, string>> | null;
  readonly IPAM: { readonly Config: readonly { readonly Subnet?: string }[] | null };
  /** The containers on it, by id, when it is inspected by name. */
  readonly Containers?: Readonly<Record<string, { readonly Name: string }>> | null;
}

/** A network as `createNetwork` makes it. */
export interface NetworkCreate {
  readonly Name: string;
  readonly Driver: "bridge";
  readonly IPAM?: { readonly Config: readonly { readonly Subnet: string }[] };
  readonly Labels: Readonly<Record<string, string>>;
}

/** A volume, as `createVolume` makes and `listVolumes` lists it. */
export interface Volume {
  readonly Name: string;
  readonly Labels: Readonly<Record<string, string>> | null;
}

/** What is read of a container: its state, and when it last started (RFC 3339, to the nanosecond). */
export interface ContainerInspect {
  readonly Name: string;
  readonly Config: { readonly Labels: Readonly<Record<string, string>> | null };
  readonly State: { readonly Status: string; readonly Running: boolean; readonly StartedAt: string };
}

/** A container as `listContainers` lists it: its names begin with "/". */
export interface ContainerSummary {
  readonly Names: readonly string[];
  readonly Labels: Readonly<Record<string, string>>;
  /** created, running, paused, restarting, exited, removing or dead. */
  readonly State: string;
}

/** A mount of a container: a host path or a named volume, at `Target`. */
export interface Mount {
  readonly Type: "bind" | "volume";
  readonly Source: string;
  readonly Target: string;
  readonly ReadOnly?: boolean;
}

/** A container as `createContainer` makes it: as much of the Engine API's body as the local cluster sets. */
export interface ContainerCreate {
  readonly Image: string;
  readonly Hostname: string;
  readonly Cmd: readonly string[];
  readonly Env: readonly string[];
  readonly Labels: Readonly<Record<string, string>>;
  readonly ExposedPorts?: Readonly<Record<string, Readonly<Record<string, never>>>>;
  readonly HostConfig: {
    readonly Privileged: boolean;
    readonly Init: boolean;
    readonly CgroupnsMode: "private" | "host";
    readonly RestartPolicy: { readonly Name: "no" | "always" | "unless-stopped" | "on-failure" };
    readonly SecurityOpt: readonly string[];
    readonly Tmpfs: Readonly<Record<string, string>>;
    readonly Mounts: readonly Mount[];
    readonly ExtraHosts: readonly string[];
    readonly PortBindings?: Readonly<Record<string, readonly { readonly HostIp: string; readonly HostPort: string }[]>>;
  };
  readonly NetworkingConfig: {
    readonly EndpointsConfig: Readonly<Record<string, { readonly IPAMConfig: { readonly IPv4Address: string } }>>;
  };
}

/** What `exec` is given beyond the command. */
export interface ExecOptions {
  /** What the command reads on its standard input, which is closed after it. */
  readonly stdin?: string;
}

/** What a command run by `exec` came to. */
export interface ExecResult {
  readonly exitCode: number;
  readonly stdout: Buffer;
  readonly stderr: string;
}

/** Docker's Engine API, as the local cluster calls it. Objects are addressed by name. */
export class DockerEngine extends Context.Service<DockerEngine, {
  readonly version: Effect.Effect<DockerVersion, DockerError>;
  /** The image, or null when Docker has none of `reference`. */
  readonly inspectImage: (reference: string) => Effect.Effect<ImageInspect | null, DockerError>;
  /** Pulls the image `reference` names by tag or digest, from a registry that asks for no login. */
  readonly pullImage: (reference: string) => Effect.Effect<void, DockerError>;
  /** The network, or null when there is none. */
  readonly inspectNetwork: (name: string) => Effect.Effect<NetworkInspect | null, DockerError>;
  /** Every network there is. */
  readonly listNetworks: Effect.Effect<readonly NetworkInspect[], DockerError>;
  /** Makes it; fails with status 409 when it exists. */
  readonly createNetwork: (network: NetworkCreate) => Effect.Effect<void, DockerError>;
  /** Removes it; one already gone is not an error. */
  readonly removeNetwork: (name: string) => Effect.Effect<void, DockerError>;
  /** Takes the container off the network, running or not. */
  readonly disconnectNetwork: (network: string, container: string) => Effect.Effect<void, DockerError>;
  /** Makes it, unless it exists. */
  readonly createVolume: (volume: Volume) => Effect.Effect<void, DockerError>;
  /** The volumes that carry every one of `labels`. */
  readonly listVolumes: (labels: Readonly<Record<string, string>>) => Effect.Effect<readonly Volume[], DockerError>;
  /** Removes it, with what it holds; one already gone is not an error. */
  readonly removeVolume: (name: string) => Effect.Effect<void, DockerError>;
  /** The containers, running or not, that carry every one of `labels`. */
  readonly listContainers: (labels: Readonly<Record<string, string>>) => Effect.Effect<readonly ContainerSummary[], DockerError>;
  /** The container, or null when there is none. */
  readonly inspectContainer: (name: string) => Effect.Effect<ContainerInspect | null, DockerError>;
  /** Makes it, stopped; fails with status 409 when it exists. */
  readonly createContainer: (name: string, container: ContainerCreate) => Effect.Effect<void, DockerError>;
  /** Starts it; one running is not an error. */
  readonly startContainer: (name: string) => Effect.Effect<void, DockerError>;
  /** Stops it, as Docker does (SIGTERM, then SIGKILL after its timeout); one stopped is not an error. */
  readonly stopContainer: (name: string) => Effect.Effect<void, DockerError>;
  /** Removes it, running or not, keeping its named volumes; one already gone is not an error. */
  readonly removeContainer: (name: string) => Effect.Effect<void, DockerError>;
  /**
   * Runs `command` in the running container, with `stdin` on its standard input when
   * given, and collects its output. Interrupting it closes the exec's connection.
   */
  readonly exec: (container: string, command: readonly string[], options?: ExecOptions) => Effect.Effect<ExecResult, DockerError>;
  /** The content of the file at `path` in the container. */
  readonly readFile: (container: string, path: string) => Effect.Effect<Buffer, DockerError>;
}>()("alasio/cluster/DockerEngine") {
  /** The Docker `env` names (dockerSocket). */
  static readonly layer = (env: Readonly<NodeJS.ProcessEnv> = process.env): Layer.Layer<DockerEngine, DockerHostUnsupported> =>
    Layer.effect(DockerEngine, Effect.map(Effect.fromResult(dockerSocket(env)), makeDockerEngine));
}

/** A call of the Engine API: its query's arrays are repeated parameters. */
interface DockerCall {
  readonly method: "GET" | "POST" | "DELETE";
  readonly path: string;
  readonly query?: Readonly<Record<string, string | readonly string[]>>;
  readonly body?: unknown;
}

const describe = (call: DockerCall): string => `${call.method} ${call.path}`;

const unreachable = (call: DockerCall, cause: unknown): DockerError =>
  new DockerError({ call: describe(call), reason: cause instanceof Error ? cause.message : String(cause) });

/** A label filter of a list call, in the JSON Docker's `filters` parameter takes. */
const labelFilter = (labels: Readonly<Record<string, string>>): string =>
  JSON.stringify({ label: Object.entries(labels).map(([key, value]) => `${key}=${value}`) });

/** The content of the first regular file in `tar`, a tar archive as Docker's archive endpoint sends one; null when it holds none. */
export function firstFile(tar: Buffer): Buffer | null {
  const BLOCK = 512;
  for (let offset = 0; offset + BLOCK <= tar.length;) {
    const header = tar.subarray(offset, offset + BLOCK);
    // Two zero blocks end an archive.
    if (header.every((byte) => byte === 0)) return null;
    const size = Number.parseInt(header.toString("latin1", 124, 136).replace(/\0.*$/su, "").trim() || "0", 8);
    const type = header[156];
    const start = offset + BLOCK;
    // "0", or NUL in archives older than POSIX's.
    if (type === 0x30 || type === 0) return tar.subarray(start, start + size);
    offset = start + Math.ceil(size / BLOCK) * BLOCK;
  }
  return null;
}

/**
 * The output of a command whose exec ran without a terminal: Docker sends it in frames of
 * an 8-byte header (the stream, 1 for stdout or 2 for stderr, and the size, big-endian, at
 * byte 4) and what was written.
 */
export function demultiplex(raw: Buffer): { readonly stdout: Buffer; readonly stderr: Buffer } {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  for (let offset = 0; offset + 8 <= raw.length;) {
    const size = raw.readUInt32BE(offset + 4);
    const payload = raw.subarray(offset + 8, offset + 8 + size);
    (raw[offset] === 2 ? stderr : stdout).push(payload);
    offset += 8 + size;
  }
  return { stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) };
}

/** The Engine API on the Unix socket `socketPath`. */
function makeDockerEngine(socketPath: string): DockerEngine["Service"] {
  const send = (call: DockerCall, headers: Readonly<Record<string, string>> = {}): ClientRequest => {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(call.query ?? {})) {
      for (const each of typeof value === "string" ? [value] : value) query.append(key, each);
    }
    const body = call.body === undefined ? undefined : JSON.stringify(call.body);
    const request = httpRequest({
      socketPath,
      method: call.method,
      path: `/${API_VERSION}${call.path}${query.size ? `?${query}` : ""}`,
      headers: body === undefined ? headers : { ...headers, "Content-Type": "application/json", "Content-Length": Buffer.byteLength(body) },
    });
    request.end(body);
    return request;
  };

  const readAll = (call: DockerCall, stream: Readable): Effect.Effect<Buffer, DockerError> =>
    Effect.tryPromise({ try: () => buffer(stream), catch: (cause) => unreachable(call, cause) });

  /** Docker's refusal of `call`: the status of `response`, and the message its body holds. */
  const refusal = (call: DockerCall, response: IncomingMessage): Effect.Effect<never, DockerError> =>
    readAll(call, response).pipe(
      Effect.flatMap((body) => {
        const status = response.statusCode ?? 0;
        const text = body.toString("utf8").trim();
        let reason = text || STATUS_CODES[status] || "no message";
        try {
          const parsed: unknown = JSON.parse(text);
          if (typeof parsed === "object" && parsed !== null && "message" in parsed && typeof parsed.message === "string") reason = parsed.message;
        } catch {
          // Not JSON: the text is the message.
        }
        return Effect.fail(new DockerError({ call: describe(call), status, reason }));
      }),
    );

  /** The response to `call`, open while the scope is; a status of 400 or over is its refusal (304, already so, is not). */
  const open = (call: DockerCall): Effect.Effect<IncomingMessage, DockerError, Scope.Scope> =>
    Effect.acquireRelease(
      Effect.callback<IncomingMessage, DockerError>((resume) => {
        const request = send(call);
        request.on("response", (response) => resume(Effect.succeed(response)));
        request.on("error", (cause) => resume(Effect.fail(unreachable(call, cause))));
        return Effect.sync(() => request.destroy());
      }),
      (response) => Effect.sync(() => response.destroy()),
    ).pipe(Effect.tap((response) => ((response.statusCode ?? 0) >= 400 ? refusal(call, response) : Effect.void)));

  const body = (call: DockerCall): Effect.Effect<Buffer, DockerError> =>
    Effect.scoped(Effect.flatMap(open(call), (response) => readAll(call, response)));

  /** The JSON Docker answers `call` with, as the Engine API documents it. */
  const json = <A>(call: DockerCall): Effect.Effect<A, DockerError> =>
    Effect.map(body(call), (content) => JSON.parse(content.toString("utf8")) as A);

  const orNull = <A>(effect: Effect.Effect<A, DockerError>): Effect.Effect<A | null, DockerError> =>
    effect.pipe(Effect.catchIf(hasStatus(404), () => Effect.succeed(null)));

  const orGone = (effect: Effect.Effect<unknown, DockerError>): Effect.Effect<void, DockerError> =>
    effect.pipe(Effect.asVoid, Effect.catchIf(hasStatus(404), () => Effect.void));

  const name = (value: string): string => encodeURIComponent(value);

  /**
   * Starts exec `id` attached, `stdin` written to it and then closed, and collects what it
   * writes until it ends. Docker upgrades the connection to a raw stream for this.
   */
  const attach = (id: string, stdin: ExecOptions["stdin"]): Effect.Effect<Buffer, DockerError> => {
    const call: DockerCall = { method: "POST", path: `/exec/${id}/start`, body: { Detach: false, Tty: false } };
    return Effect.callback<Buffer, DockerError>((resume) => {
      const request = send(call, { Connection: "Upgrade", Upgrade: "tcp" });
      let stream: Duplex | null = null;
      request.on("upgrade", (_response, socket, head) => {
        stream = socket;
        const chunks: Buffer[] = [head];
        socket.on("data", (chunk: Buffer) => chunks.push(chunk));
        socket.on("error", (cause) => resume(Effect.fail(unreachable(call, cause))));
        socket.on("close", () => resume(Effect.succeed(Buffer.concat(chunks))));
        if (stdin !== undefined) socket.end(stdin);
      });
      // A daemon that answers without upgrading refused.
      request.on("response", (response) => resume(refusal(call, response)));
      request.on("error", (cause) => resume(Effect.fail(unreachable(call, cause))));
      return Effect.sync(() => {
        request.destroy();
        stream?.destroy();
      });
    });
  };

  const exec = Effect.fnUntraced(function*(
    container: string,
    command: readonly string[],
    { stdin }: ExecOptions = {},
  ): Effect.fn.Return<ExecResult, DockerError> {
    const { Id: id } = yield* json<{ readonly Id: string }>({
      method: "POST",
      path: `/containers/${name(container)}/exec`,
      body: { AttachStdin: stdin !== undefined, AttachStdout: true, AttachStderr: true, Tty: false, Cmd: command },
    });
    const { stdout, stderr } = demultiplex(yield* attach(id, stdin));
    // Docker records the exit code before it closes the exec's streams.
    const inspect: DockerCall = { method: "GET", path: `/exec/${id}/json` };
    const { ExitCode: exitCode } = yield* json<{ readonly ExitCode: number | null }>(inspect);
    if (exitCode === null) return yield* new DockerError({ call: describe(inspect), reason: `${JSON.stringify(command[0])} has no exit code` });
    return { exitCode, stdout, stderr: stderr.toString("utf8") };
  });

  const pullImage = Effect.fnUntraced(function*(reference: string): Effect.fn.Return<void, DockerError> {
    const call: DockerCall = { method: "POST", path: "/images/create", query: { fromImage: reference } };
    // A pull that fails once it has begun still answers 200: its progress, one JSON object
    // a line, ends with the error.
    const progress = (yield* body(call)).toString("utf8").split("\n").filter((line) => line.trim());
    for (const line of progress) {
      const { error } = JSON.parse(line) as { readonly error?: string };
      if (error) return yield* new DockerError({ call: describe(call), reason: error });
    }
  });

  const readFile = Effect.fnUntraced(function*(container: string, path: string): Effect.fn.Return<Buffer, DockerError> {
    const call: DockerCall = { method: "GET", path: `/containers/${name(container)}/archive`, query: { path } };
    const file = firstFile(yield* body(call));
    if (!file) return yield* new DockerError({ call: describe(call), reason: `${path} in ${container} is not a file` });
    return file;
  });

  return DockerEngine.of({
    version: json({ method: "GET", path: "/version" }),
    // A reference as it is: the route takes its slashes and colons.
    inspectImage: (reference) => orNull(json({ method: "GET", path: `/images/${reference}/json` })),
    pullImage,
    inspectNetwork: (network) => orNull(json({ method: "GET", path: `/networks/${name(network)}` })),
    listNetworks: json({ method: "GET", path: "/networks" }),
    createNetwork: (network) => Effect.asVoid(body({ method: "POST", path: "/networks/create", body: network })),
    removeNetwork: (network) => orGone(body({ method: "DELETE", path: `/networks/${name(network)}` })),
    disconnectNetwork: (network, container) =>
      Effect.asVoid(body({ method: "POST", path: `/networks/${name(network)}/disconnect`, body: { Container: container, Force: true } })),
    createVolume: (volume) => Effect.asVoid(body({ method: "POST", path: "/volumes/create", body: volume })),
    listVolumes: (labels) =>
      json<{ readonly Volumes: readonly Volume[] | null }>({ method: "GET", path: "/volumes", query: { filters: labelFilter(labels) } }).pipe(
        Effect.map(({ Volumes }) => Volumes ?? []),
      ),
    removeVolume: (volume) => orGone(body({ method: "DELETE", path: `/volumes/${name(volume)}` })),
    listContainers: (labels) => json({ method: "GET", path: "/containers/json", query: { all: "true", filters: labelFilter(labels) } }),
    inspectContainer: (container) => orNull(json({ method: "GET", path: `/containers/${name(container)}/json` })),
    createContainer: (container, spec) => Effect.asVoid(body({ method: "POST", path: "/containers/create", query: { name: container }, body: spec })),
    startContainer: (container) => Effect.asVoid(body({ method: "POST", path: `/containers/${name(container)}/start` })),
    stopContainer: (container) => Effect.asVoid(body({ method: "POST", path: `/containers/${name(container)}/stop` })),
    // Asked again while Docker has killed it but not seen it exit, for as long as a
    // node's dying processes may take.
    removeContainer: (container) =>
      orGone(
        body({ method: "DELETE", path: `/containers/${name(container)}`, query: { force: "true" } }).pipe(
          Effect.retry({ while: killedNotYetExited, schedule: Schedule.max([Schedule.spaced("1 second"), Schedule.during("2 minutes")]) }),
        ),
      ),
    exec,
    readFile,
  });
}
