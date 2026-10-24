/**
 * This machine made a node of k3s, as the cluster alasio makes on it outside Docker is
 * (./host.ts): k3s installed by its own install script, as the systemd service `k3s`, from
 * the binary cluster/node/pins.json pins, with each gVisor release it pins beside it, each
 * a runtime of containerd's (./k3s.ts), and stargz-snapshotter's daemon, the service
 * `stargz-snapshotter`, its snapshotter (./stargz.ts), and set in
 * /etc/rancher/k3s/config.yaml. The node image (cluster/node) is built from the same pins,
 * its gVisor releases where the machine has them, and its k3s reads the same containerd
 * files, so a node in Docker and one on a machine run the same k3s, gVisor and snapshotter
 * alike.
 *
 * Every download is checked against its pin as the operator downloads it, and again as
 * root before it is installed. What alasio installed, and the digest of the files that
 * configure the node, are in /etc/rancher/k3s/alasio.json, which everyone reads, so a
 * command not run as root knows what changes and asks root for that alone; and k3s's own
 * uninstall script, which removes /etc/rancher/k3s, removes it too.
 *
 * The steps here are each one's own work as root (./root.ts), the same for any machine
 * made a node: a server here, which NodeConfig's role says, as an agent of another
 * machine's server would.
 */
import { createHash, randomUUID } from "node:crypto";
import { dirname } from "node:path";
import { gunzipSync, zstdDecompressSync } from "node:zlib";

import { Duration, Effect, FileSystem, Option, type PlatformError, Schedule, Schema } from "effect";

import pins from "../../../cluster/node/pins.json" with { type: "json" };
import { CONTAINERD, GVISOR_RELEASES, gvisorDirectory, gvisorHandler, NODE_DIRECTORY } from "../../../src/node-agent/names.ts";
import { Registries } from "../config.ts";
import { clusterKubeconfig, containerdFiles, KUBELET_ARGS, KUBELET_CONFIG, KUBELET_CONFIG_PATH, NotYet, registriesYaml, SANDBOX_IMAGE } from "./k3s.ts";
import { Machine, type RootCommandFailed, RootSystem, under } from "./machine.ts";
import { FUSE_MANAGER_SOCKET, FUSE_MANAGER_STORE, STARGZ_CONFIG, STARGZ_EXECUTABLES, STARGZ_MEMORY_LIMIT, STARGZ_ROOT, STARGZ_SOCKET, stargzConfig } from "./stargz.ts";
import { tarEntries } from "./tar.ts";

/**
 * The k3s, gVisor and stargz-snapshotter a node runs, pinned: k3s's release, the gVisor
 * releases alasio keeps, the current one first, which new sandboxes run, then at most the
 * one before it, which sandboxes snapshotted under it are restored with, and
 * stargz-snapshotter's release.
 */
export type NodePins = typeof pins;

/** This version's pins, cluster/node/pins.json's. */
export const NODE_PINS: NodePins = pins;

/** The gVisor releases `pins` pins, the current first. */
export const gvisorReleases = ({ gvisor }: NodePins): string[] => gvisor.releases.map(({ release }) => release);

/** The gVisor release `pins` pins as current, which new sandboxes run: the first; pins of none are no pins. */
export function currentGvisorRelease(pins: NodePins = NODE_PINS): string {
  const [current] = gvisorReleases(pins);
  if (current === undefined) throw new Error("cluster/node/pins.json pins no gVisor release");
  return current;
}

/** A file to download, and the digest it must have. */
export interface Download {
  readonly url: string;
  readonly algorithm: "sha256" | "sha512";
  readonly digest: string;
}

/**
 * Where what `pins` pins is downloaded from: k3s's binary and install script from its
 * repository, each gVisor release, by its name, from alasio's fork of it,
 * github.com/eaucoin/gvisor, and stargz-snapshotter's from its repository.
 */
export const downloads = ({ k3s, gvisor, stargz }: NodePins): {
  readonly k3s: Download;
  readonly installScript: Download;
  readonly gvisor: ReadonlyMap<string, Download>;
  readonly stargz: Download;
} => ({
  k3s: { url: `https://github.com/k3s-io/k3s/releases/download/${encodeURIComponent(k3s.version)}/k3s`, algorithm: "sha256", digest: k3s.sha256 },
  installScript: { url: `https://raw.githubusercontent.com/k3s-io/k3s/${k3s.installScript.commit}/install.sh`, algorithm: "sha256", digest: k3s.installScript.sha256 },
  gvisor: new Map(gvisor.releases.map(({ release, sha512 }) => [
    release,
    { url: `https://github.com/eaucoin/gvisor/releases/download/release-${encodeURIComponent(release)}/gvisor-x86_64.tar.zstd`, algorithm: "sha512", digest: sha512 },
  ])),
  stargz: {
    url: `https://github.com/containerd/stargz-snapshotter/releases/download/${encodeURIComponent(stargz.version)}/stargz-snapshotter-${encodeURIComponent(stargz.version)}-linux-amd64.tar.gz`,
    algorithm: "sha256",
    digest: stargz.sha256,
  },
});

const BIN = "/usr/local/bin";
const K3S_ETC = "/etc/rancher/k3s";
const CONFIG_YAML = `${K3S_ETC}/config.yaml`;
const REGISTRIES_YAML = `${K3S_ETC}/registries.yaml`;
/**
 * What alasio installed, and the digest of the files that configure the node. Not the
 * CPU baseline sessions are held to, which is the cluster's, of every node of it, and
 * recorded in it (../cpu-baseline.ts): a cluster in Docker or of a kubeconfig has no
 * stamp, and the other nodes of a cluster on this machine are not described by its stamp.
 */
const STAMP = `${K3S_ETC}/alasio.json`;
/** The kubeconfig k3s writes, root's alone. */
const K3S_KUBECONFIG = `${K3S_ETC}/k3s.yaml`;
/** The systemd service k3s's install script makes. */
export const SERVICE = "k3s";
const SERVICE_FILE = `/etc/systemd/system/${SERVICE}.service`;
/** Where k3s keeps the password its node registers with. */
const NODE_PASSWORD = "/etc/rancher/node";
const KILLALL = `${BIN}/k3s-killall.sh`;
const UNINSTALL = `${BIN}/k3s-uninstall.sh`;
/** The networks of the cluster's pods and Services, as k3s is set with them: k3s's own. */
const CLUSTER_CIDR = "10.42.0.0/16";
const SERVICE_CIDR = "10.43.0.0/16";
/**
 * What a firewall must let in to the machine from the cluster: its pods and its Services,
 * as k3s's requirements say; and nothing more. The webhook intake's port is no rule of a
 * firewall's: what reaches a pod's host port kube-router's NetworkPolicy chain accepts in
 * FORWARD ahead of ufw's or firewalld's rules, which never see it, so the intake's
 * NetworkPolicy, admitting webhook.sources alone, is what guards it
 * (cli/src/manifests/network-policies.ts).
 */
export const CLUSTER_NETWORKS: readonly string[] = [CLUSTER_CIDR, SERVICE_CIDR];
/** What alasio installs of a gVisor release, in the release's directory: runsc and its shim, and the binaries runsc runs beside it. */
const GVISOR = ["runsc", "containerd-shim-runsc-v1", "gvisor-bin"] as const;
/** stargz-snapshotter's daemon's systemd service, which k3s's wants, and with which it stops and restarts. */
export const STARGZ_SERVICE = "stargz-snapshotter";
const STARGZ_UNIT = `/etc/systemd/system/${STARGZ_SERVICE}.service`;
/** What k3s's service is given beside its install script's: the daemon, started before it. */
const K3S_DROP_IN = `/etc/systemd/system/${SERVICE}.service.d/alasio-stargz.conf`;
/** The containers that ran on a FUSE manager gone, which the daemon's service lists as it starts and stops once the daemon serves, one id a line. */
const ON_GONE_MANAGER = `${dirname(STARGZ_SOCKET)}/on-gone-manager`;
/** The mounts of a FUSE manager gone, as the daemon's service finds them before it unmounts them. */
const SERVED = `${ON_GONE_MANAGER}.mounts`;

/**
 * How a node is made: a server, its volumes in `storagePath`, pulling through
 * `registries`, its containerd running each of the gVisor releases `gvisor`, the current
 * first.
 */
export const NodeConfig = Schema.Struct({
  role: Schema.Literal("server"),
  storagePath: Schema.String,
  registries: Schema.optionalKey(Registries),
  gvisor: Schema.Array(Schema.String),
});
export type NodeConfig = typeof NodeConfig.Type;

/**
 * The files that make k3s and its containerd a node of `config`, by their paths: k3s's
 * config.yaml, written as JSON, which YAML reads as it is, its snapshotter none, as
 * containerd's files say the daemon's, and its sandbox image pinned (./k3s.ts); its kubelet's
 * config file; the registries; the files that make containerd pull through stargz-snapshotter's daemon and
 * run each gVisor release, where systemd runs the machine; and the daemon's configuration
 * and service, which k3s's service wants, and with which it stops and restarts. Pure, for
 * tests.
 */
export function nodeFiles(config: NodeConfig): ReadonlyMap<string, string> {
  const k3s = {
    "disable": ["traefik"],
    "cluster-cidr": CLUSTER_CIDR,
    "service-cidr": SERVICE_CIDR,
    "default-local-storage-path": config.storagePath,
    "kubelet-arg": KUBELET_ARGS,
    "snapshotter": "",
    "pause-image": SANDBOX_IMAGE,
  };
  return new Map([
    [CONFIG_YAML, `# k3s as alasio's cluster runs it: alasio up writes this file.\n${JSON.stringify(k3s)}\n`],
    [KUBELET_CONFIG_PATH, KUBELET_CONFIG],
    ...(config.registries ? [[REGISTRIES_YAML, `${registriesYaml(config.registries)}\n`] as const] : []),
    ...containerdFiles(config.gvisor, { systemd: true }),
    [STARGZ_CONFIG, stargzConfig(config.registries)],
    [STARGZ_UNIT, [
      "# stargz-snapshotter's daemon, the snapshotter of k3s's containerd: alasio up writes this file.",
      "[Unit]",
      "Description=stargz-snapshotter's daemon, which k3s's containerd pulls images through",
      `PartOf=${SERVICE}.service`,
      "",
      "[Service]",
      "# Go's soft limit on its heap and its FUSE manager's (./stargz.ts's STARGZ_MEMORY_LIMIT).",
      `Environment=GOMEMLIMIT=${STARGZ_MEMORY_LIMIT}`,
      "# Its FUSE manager gone, as one killed is or a machine's crash ends, its socket held by no process, as the",
      "# kernel's table of sockets says: its socket removed, so the daemon starts a manager anew rather than none for the",
      "# socket; its record of what it mounted removed, so the new manager mounts nothing the daemon then finds mounted",
      "# and skips (./stargz.ts's FUSE_MANAGER_SOCKET and FUSE_MANAGER_STORE); and the mounts it served unmounted",
      "# lazily, deepest first, as the daemon unmounts each before it mounts it again, and cannot where the pods'",
      "# containers that ran on it hold it. And those containers, each on an overlay of a layer it served, whose reads",
      "# fail now and whose probes may not say so, listed, the id of each the directory of its root's mount, to be",
      "# stopped once the daemon serves. Only then: a manager there serves its mounts on, and is left be with them and",
      "# its pods, so what is unmounted serves nothing already.",
      `ExecStartPre=/bin/sh -c '${[
        // systemd's $$ a $, the end of the line the socket is on.
        `grep -q " ${FUSE_MANAGER_SOCKET}$$" /proc/net/unix && exit 0`,
        `mkdir -p ${dirname(STARGZ_SOCKET)}`,
        // Its mounts, the layers it served; the overlays on any of them, the containers that ran on it.
        `grep " - fuse" /proc/self/mountinfo | cut -d " " -f 5 | grep "^${STARGZ_ROOT}/snapshotter/snapshots/" > ${SERVED}`,
        `grep " - overlay " /proc/self/mountinfo | grep -F -f ${SERVED} | cut -d " " -f 5 | sed -n "s|/rootfs$$||p" | xargs -r -n 1 basename > ${ON_GONE_MANAGER}`,
        // The daemon's socket too, which it makes once it has mounted its snapshots' layers again, and the step after waits for.
        `rm -f ${FUSE_MANAGER_SOCKET} ${FUSE_MANAGER_STORE} ${STARGZ_SOCKET}`,
        `sort -r ${SERVED} | xargs -r -n 1 umount -l`,
        `rm -f ${SERVED}`,
      ].join("; ")}'`,
      `ExecStart=${BIN}/containerd-stargz-grpc --address=${STARGZ_SOCKET} --config=${STARGZ_CONFIG} --root=${STARGZ_ROOT}`,
      "# The containers listed before it started stopped at once, once it serves, as no hook of theirs can run on what",
      "# serves nothing, so kubelet starts each again on the layers it mounted anew; what it costs, the containers on",
      "# those mounts started again, they could read nothing anyway. Each step bounded, and the unit started however",
      "# they end, a stop that fails said in its journal. Where the manager went as k3s restarts, k3s's start waits for",
      "# this one's (After=), about 65 s at most, and crictl reaches no containerd: the stop fails so, and those",
      "# containers run on, their reads failing, until each ends or is ended by hand.",
      `ExecStartPost=-/bin/sh -c '${[
        `[ -s ${ON_GONE_MANAGER} ] || { rm -f ${ON_GONE_MANAGER}; exit 0; }`,
        `waited=0; until [ -S ${STARGZ_SOCKET} ] || [ "$$waited" -ge 450 ]; do sleep 0.1; waited=$$((waited + 1)); done`,
        `timeout 20 xargs -n 1 ${BIN}/k3s crictl stop --timeout 0 < ${ON_GONE_MANAGER} || echo "stopping the containers on the FUSE manager gone failed: $$(xargs < ${ON_GONE_MANAGER})" >&2`,
        `rm -f ${ON_GONE_MANAGER}`,
      ].join("; ")}'`,
      "Restart=always",
      "# Its FUSE manager keeps the mounts of the images running pods read, through the daemon's restarts.",
      "KillMode=process",
      "",
    ].join("\n")],
    [K3S_DROP_IN, [
      "# k3s's service, its images pulled through stargz-snapshotter's daemon: alasio up writes this file.",
      "[Unit]",
      `Wants=${STARGZ_SERVICE}.service`,
      `After=${STARGZ_SERVICE}.service`,
      "",
    ].join("\n")],
  ]);
}

/** A digest of `files` that two sets of files share only when they are the same. */
export const filesDigest = (files: ReadonlyMap<string, string>): string => createHash("sha256").update(JSON.stringify([...files])).digest("hex");

/** A firewall alasio opens the cluster's networks in, where it is active. */
export const Firewall = Schema.Literals(["ufw", "firewalld"]);
export type Firewall = typeof Firewall.Type;

/**
 * What alasio installed on this machine, as STAMP records it: k3s's version, gVisor's
 * releases, stargz-snapshotter's version, the digest of the files that configure the node,
 * and the firewall it opened the cluster's networks in, with those it added a rule for
 * itself, which are all it removes.
 */
export const NodeStamp = Schema.Struct({
  k3s: Schema.optionalKey(Schema.String),
  gvisor: Schema.optionalKey(Schema.Array(Schema.String)),
  stargz: Schema.optionalKey(Schema.String),
  config: Schema.optionalKey(Schema.String),
  firewall: Schema.optionalKey(Schema.Struct({ tool: Firewall, networks: Schema.Array(Schema.String), added: Schema.Array(Schema.String) })),
});
export type NodeStamp = typeof NodeStamp.Type;

/** What alasio installed, or null when STAMP is not there: alasio installed nothing, or k3s's uninstall script removed it. One that cannot be read records nothing. */
export const readStamp: Effect.Effect<NodeStamp | null, PlatformError.PlatformError, FileSystem.FileSystem | Machine> = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  const path = under(root, STAMP);
  if (!(yield* fs.exists(path))) return null;
  const text = yield* fs.readFileString(path);
  return Option.getOrElse(Schema.decodeUnknownOption(Schema.fromJsonString(NodeStamp))(text), (): NodeStamp => ({}));
});

/** STAMP with `changes`, readable by everyone. */
const recordStamp = Effect.fnUntraced(function*(changes: NodeStamp): Effect.fn.Return<void, PlatformError.PlatformError, FileSystem.FileSystem | Machine> {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  yield* fs.makeDirectory(under(root, K3S_ETC), { recursive: true });
  yield* writeFile(under(root, STAMP), JSON.stringify({ ...(yield* readStamp), ...changes }), 0o644);
});

/** A file downloaded, where it is, and what it is checked against. */
export const Artifact = Schema.Struct({
  path: Schema.String,
  url: Schema.String,
  algorithm: Schema.Literals(["sha256", "sha512"]),
  digest: Schema.String,
});
export type Artifact = typeof Artifact.Type;

/** A file is not what its pin says. */
export class ChecksumMismatch extends Schema.TaggedError<ChecksumMismatch>()("ChecksumMismatch", {
  url: Schema.String,
  algorithm: Schema.String,
  expected: Schema.String,
  actual: Schema.String,
}) {
  override get message(): string {
    return `${this.url} is not what alasio pins: its ${this.algorithm} is ${this.actual}, not ${this.expected}`;
  }
}

/** `content`, which must be `download`'s, by its digest. */
export const checked = (download: Download, content: Uint8Array): Effect.Effect<Uint8Array, ChecksumMismatch> => {
  const actual = createHash(download.algorithm).update(content).digest("hex");
  return actual === download.digest ? Effect.succeed(content) : Effect.fail(new ChecksumMismatch({ ...download, expected: download.digest, actual }));
};

/** The content of `artifact`, checked against its digest as it is read. */
const verified = (artifact: Artifact): Effect.Effect<Uint8Array, ChecksumMismatch | PlatformError.PlatformError, FileSystem.FileSystem> =>
  Effect.flatMap(FileSystem.FileSystem, (fs) => Effect.flatMap(fs.readFile(artifact.path), (content) => checked(artifact, content)));

/** Writes `content` to `path` whole, beside it, then moves it over it, as a running executable cannot be written to. */
const writeFile = Effect.fnUntraced(function*(path: string, content: string | Uint8Array, mode: number): Effect.fn.Return<void, PlatformError.PlatformError, FileSystem.FileSystem> {
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(dirname(path), { recursive: true });
  const written = `${path}.${randomUUID()}`;
  if (typeof content === "string") yield* fs.writeFileString(written, content, { flag: "wx", mode });
  else yield* fs.writeFile(written, content, { flag: "wx", mode });
  yield* fs.chmod(written, mode);
  yield* fs.rename(written, path);
});

/** The step that lets `networks`, the cluster's, in to the machine through `firewall`, which is active. */
export const OpenFirewall = Schema.TaggedStruct("OpenFirewall", { firewall: Firewall, networks: Schema.Array(Schema.String) });
/** The step that writes the files that configure k3s and its containerd as a node of `config`. */
export const ConfigureNode = Schema.TaggedStruct("ConfigureNode", { config: NodeConfig });
/** The step that installs a gVisor release, from its archive, in a directory of its own. */
export const InstallGvisor = Schema.TaggedStruct("InstallGvisor", { release: Schema.String, archive: Artifact });
/** The step that stops a gVisor release's sandboxes, and removes it. */
export const RemoveGvisor = Schema.TaggedStruct("RemoveGvisor", { release: Schema.String });
/** The step that installs stargz-snapshotter's daemon and its FUSE manager, from its release's archive, in place of `previous`. */
export const InstallStargz = Schema.TaggedStruct("InstallStargz", { version: Schema.String, previous: Schema.optionalKey(Schema.String), archive: Artifact });
/** The step that installs k3s's binary, with its install script, which makes its service, enables it, and starts it anew. */
export const InstallK3s = Schema.TaggedStruct("InstallK3s", {
  version: Schema.String,
  previous: Schema.optionalKey(Schema.String),
  role: Schema.Literal("server"),
  binary: Artifact,
  script: Artifact,
});
/** The step that starts k3s's service, enabling it, or restarts it. */
export const StartK3s = Schema.TaggedStruct("StartK3s", { restart: Schema.Boolean });
/** The step that stops k3s's service, disabling it, and stops its pods. */
export const StopK3s = Schema.TaggedStruct("StopK3s", {});
/** The step that removes k3s, with its uninstall script, gVisor's releases, and the firewall's rules for the cluster's networks alasio added, by STAMP's `firewall`. */
export const RemoveNode = Schema.TaggedStruct("RemoveNode", { firewall: Schema.optionalKey(Schema.Struct({ tool: Firewall, added: Schema.Array(Schema.String) })) });
/** The step that removes the directory of the node's volumes. */
export const RemoveStorage = Schema.TaggedStruct("RemoveStorage", { path: Schema.String });
/** The step that reads k3s's kubeconfig, after restarting k3s, which renews its certificates, when `renew`. */
export const ReadKubeconfig = Schema.TaggedStruct("ReadKubeconfig", { renew: Schema.Boolean });

export const NodeStep = Schema.Union([OpenFirewall, ConfigureNode, InstallGvisor, RemoveGvisor, InstallStargz, InstallK3s, StartK3s, StopK3s, RemoveNode, RemoveStorage, ReadKubeconfig]);
export type NodeStep = typeof NodeStep.Type;

/** What `step` does, as alasio says it will. */
export function describeNodeStep(step: NodeStep): string {
  const replacing = (previous: string | undefined) => (previous ? `, in place of ${previous},` : "");
  switch (step._tag) {
    case "OpenFirewall":
      return `let the cluster's pods and Services, ${step.networks.join(" and ")}, in to this machine through ${step.firewall}, as k3s needs; ` +
        "nothing else is opened, k3s's API server's port 6443 staying closed to the rest";
    case "ConfigureNode": {
      const runtimes = step.config.gvisor.map(gvisorHandler);
      return `write k3s's configuration in ${K3S_ETC}, and its containerd's, with gVisor as the runtime${runtimes.length > 1 ? "s" : ""} ${runtimes.join(" and ")}, in ${CONTAINERD}, ` +
        `and stargz-snapshotter's daemon's, as the service ${STARGZ_SERVICE}, which k3s's starts`;
    }
    case "InstallGvisor":
      return `install gVisor ${step.release} in ${gvisorDirectory(step.release)}: ${GVISOR.join(", ")}`;
    case "RemoveGvisor":
      return `stop the sandboxes of gVisor ${step.release}, which alasio no longer keeps, and remove ${gvisorDirectory(step.release)}`;
    case "InstallStargz":
      return `install stargz-snapshotter ${step.version}${replacing(step.previous)} in ${BIN}: ${STARGZ_EXECUTABLES.join(", ")}`;
    case "InstallK3s":
      return `install k3s ${step.version}${replacing(step.previous)} in ${BIN}, with its own install script, as the systemd service ${SERVICE}, enabled and started anew`;
    case "StartK3s":
      return step.restart ? `restart the service ${SERVICE}, with its new configuration` : `enable and start the service ${SERVICE}`;
    case "StopK3s":
      return `stop and disable the service ${SERVICE}, and stop the cluster's pods, gVisor's and those k3s-killall.sh stops`;
    case "RemoveNode":
      return `stop the cluster's gVisor pods, ` +
        (step.firewall && step.firewall.added.length > 0 ? `remove the rules alasio added to ${step.firewall.tool} for ${step.firewall.added.join(" and ")}, ` : "") +
        `uninstall k3s with k3s-uninstall.sh, which removes its service, ${K3S_ETC} and its data, ` +
        `and remove what it leaves, its node's password in ${NODE_PASSWORD}, gVisor's releases in ${GVISOR_RELEASES}, stargz-snapshotter from ${BIN}, ` +
        `stargz-snapshotter's service, its mounts and its images in ${STARGZ_ROOT} and its sockets in ${dirname(STARGZ_SOCKET)}, and alasio's mounts and directory, ${NODE_DIRECTORY}`;
    case "RemoveStorage":
      return `remove ${step.path}, the cluster's volumes`;
    case "ReadKubeconfig":
      return `${step.renew ? `restart the service ${SERVICE}, which renews its certificates, and ` : ""}read its kubeconfig, ${K3S_KUBECONFIG}, for alasio to reach it`;
  }
}

/** How a node's step fails. */
export type NodeStepError = PlatformError.PlatformError | ChecksumMismatch | RootCommandFailed | NotYet;

/** The services a node's step runs with. */
type NodeServices = FileSystem.FileSystem | Machine | RootSystem;

/** The names in the directory `path` of this machine; none when it is not there. */
const namesIn = Effect.fnUntraced(function*(path: string): Effect.fn.Return<string[], PlatformError.PlatformError, FileSystem.FileSystem | Machine> {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  return (yield* fs.exists(under(root, path))) ? yield* fs.readDirectory(under(root, path)) : [];
});

/** The gVisor releases installed on this machine, by their directories. */
const installedGvisor = namesIn(GVISOR_RELEASES);

/** The executables of gVisor's `release`, as this machine names them, which its processes run. */
const gvisorExecutables = Effect.fnUntraced(function*(release: string): Effect.fn.Return<string[], PlatformError.PlatformError, FileSystem.FileSystem | Machine> {
  const directory = gvisorDirectory(release);
  const beside = (yield* namesIn(`${directory}/gvisor-bin`)).map((name) => `${directory}/gvisor-bin/${name}`);
  return [`${directory}/runsc`, `${directory}/containerd-shim-runsc-v1`, ...beside];
});

/** Kills the processes of gVisor's `releases`, their sandboxes and shims, which k3s's own scripts do not stop. */
const killGvisor = Effect.fnUntraced(function*(releases: readonly string[]): Effect.fn.Return<void, PlatformError.PlatformError, NodeServices> {
  const killed = yield* (yield* RootSystem).kill((yield* Effect.forEach(releases, gvisorExecutables)).flat());
  if (killed > 0) yield* Effect.logInfo(`stopped ${killed} of gVisor's processes`);
});

/**
 * Unmounts what is mounted in `directories`, at once, as their FUSE servers are gone with
 * the cluster's pods and stargz-snapshotter's FUSE manager, then removes them. A mount
 * point's spaces and the like are octal escapes in /proc/self/mounts.
 */
const removeMounted = (directories: readonly string[]) =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const { root } = yield* Machine;
    const table = under(root, "/proc/self/mounts");
    const mounts = (yield* fs.exists(table)) ? (yield* fs.readFileString(table)).split("\n").map((line) => (line.split(" ")[1] ?? "").replace(/\\(\d{3})/gu, (_, code: string) => String.fromCharCode(Number.parseInt(code, 8)))) : [];
    const inside = mounts.filter((point) => directories.some((directory) => point === directory || point.startsWith(`${directory}/`))).toSorted().toReversed();
    for (const point of inside) yield* (yield* RootSystem).run("umount", ["--lazy", point]);
    for (const directory of directories) yield* fs.remove(under(root, directory), { recursive: true, force: true });
  });

/** What a firewall alasio opens the cluster's networks in is asked, and told, as root. */
const FIREWALLS: Readonly<Record<Firewall, {
  /** Whether `network` is let in already, by a rule of the firewall's own, alasio's or another's. */
  readonly lets: (network: string) => Effect.Effect<boolean, RootCommandFailed, RootSystem>;
  readonly allow: (network: string) => Effect.Effect<void, RootCommandFailed, RootSystem>;
  readonly remove: (network: string) => Effect.Effect<void, RootCommandFailed, RootSystem>;
  /** Makes what it was told hold now, after it was told it; nothing for one it holds at once. */
  readonly apply: Effect.Effect<void, RootCommandFailed, RootSystem>;
}>> = {
  // ufw's rules hold as they are added, and `ufw show added` says each as `ufw allow from NETWORK`.
  ufw: {
    lets: (network) =>
      Effect.flatMap(RootSystem, (system) => system.ask("ufw", ["show", "added"])).pipe(
        Effect.map(({ stdout }) => stdout.split("\n").some((line) => line.trim() === `ufw allow from ${network}`)),
      ),
    allow: (network) => Effect.flatMap(RootSystem, (system) => system.run("ufw", ["allow", "from", network, "to", "any"])),
    remove: (network) => Effect.flatMap(RootSystem, (system) => system.run("ufw", ["delete", "allow", "from", network, "to", "any"])),
    apply: Effect.void,
  },
  // firewalld's trusted zone lets its sources in, its permanent rules holding once reloaded.
  firewalld: {
    lets: (network) =>
      Effect.flatMap(RootSystem, (system) => system.ask("firewall-cmd", ["--permanent", "--zone=trusted", `--query-source=${network}`])).pipe(
        Effect.map(({ exitCode }) => exitCode === 0),
      ),
    allow: (network) => Effect.flatMap(RootSystem, (system) => system.run("firewall-cmd", ["--permanent", "--zone=trusted", `--add-source=${network}`])),
    remove: (network) => Effect.flatMap(RootSystem, (system) => system.run("firewall-cmd", ["--permanent", "--zone=trusted", `--remove-source=${network}`])),
    apply: Effect.flatMap(RootSystem, (system) => system.run("firewall-cmd", ["--reload"])),
  },
};

/** Removes the rules for `added` from `tool`, those alasio added, and no other. */
const closeFirewall = ({ tool, added }: { readonly tool: Firewall; readonly added: readonly string[] }) =>
  Effect.gen(function*() {
    if (added.length === 0) return;
    for (const network of added) yield* FIREWALLS[tool].remove(network);
    yield* FIREWALLS[tool].apply;
    yield* Effect.logInfo(`removed ${tool}'s rules for ${added.join(" and ")}`);
  });

/** Whether k3s's service is installed. */
const serviceInstalled = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  return yield* fs.exists(under(root, SERVICE_FILE));
});

const systemctl = (...args: string[]) => Effect.flatMap(RootSystem, (system) => system.run("systemctl", args));

/** The kubeconfig k3s wrote, once it is one. */
const kubeconfigWritten = Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem;
  const { root } = yield* Machine;
  const path = under(root, K3S_KUBECONFIG);
  const content = (yield* fs.exists(path)) ? yield* fs.readFileString(path) : "";
  if (clusterKubeconfig(content, SERVICE) === null) return yield* new NotYet({ message: `${K3S_KUBECONFIG} is not yet a kubeconfig with a cluster and a user` });
  return content;
});

/** Does `step`, as root: k3s's kubeconfig, when it reads it. */
export const performNodeStep = (step: NodeStep): Effect.Effect<string | void, NodeStepError, NodeServices> =>
  Effect.gen(function*() {
    const fs = yield* FileSystem.FileSystem;
    const { root } = yield* Machine;
    const system = yield* RootSystem;
    switch (step._tag) {
      case "OpenFirewall": {
        const firewall = FIREWALLS[step.firewall];
        const previous = (yield* readStamp)?.firewall;
        const added = previous?.tool === step.firewall ? [...previous.added] : [];
        for (const network of step.networks) {
          if (yield* firewall.lets(network)) continue;
          yield* firewall.allow(network);
          added.push(network);
        }
        yield* firewall.apply;
        yield* recordStamp({ firewall: { tool: step.firewall, networks: step.networks, added } });
        return yield* Effect.logInfo(`${step.firewall} lets ${step.networks.join(" and ")} in`);
      }
      case "ConfigureNode": {
        const files = nodeFiles(step.config);
        for (const [path, content] of files) yield* writeFile(under(root, path), content, path.startsWith(K3S_ETC) ? 0o600 : 0o644);
        if (!files.has(REGISTRIES_YAML)) yield* fs.remove(under(root, REGISTRIES_YAML), { force: true });
        // runsc's options for the releases containerd no longer runs; containerd's own config.toml, k3s's, stays.
        for (const name of yield* namesIn(CONTAINERD)) {
          const path = `${CONTAINERD}/${name}`;
          if (name.startsWith("runsc") && name.endsWith(".toml") && !files.has(path)) yield* fs.remove(under(root, path));
        }
        // The daemon's service and k3s's drop-in, as systemd reads them anew.
        yield* systemctl("daemon-reload");
        yield* recordStamp({ config: filesDigest(files) });
        return yield* Effect.logInfo(`wrote ${[...files.keys()].join(", ")}`);
      }
      case "InstallGvisor": {
        const entries = tarEntries(Buffer.from(zstdDecompressSync(yield* verified(step.archive))));
        for (const entry of entries.filter(({ name }) => GVISOR.some((installed) => name === installed || name.startsWith(`${installed}/`)))) {
          const path = under(root, `${gvisorDirectory(step.release)}/${entry.name.replace(/\/$/u, "")}`);
          if (entry.type === "directory") yield* fs.makeDirectory(path, { recursive: true, mode: entry.mode });
          else yield* writeFile(path, entry.content, entry.mode);
        }
        yield* recordStamp({ gvisor: [...((yield* readStamp)?.gvisor ?? []).filter((release) => release !== step.release), step.release] });
        return yield* Effect.logInfo(`installed gVisor ${step.release}`);
      }
      case "RemoveGvisor": {
        yield* killGvisor([step.release]);
        yield* fs.remove(under(root, gvisorDirectory(step.release)), { recursive: true, force: true });
        yield* recordStamp({ gvisor: ((yield* readStamp)?.gvisor ?? []).filter((release) => release !== step.release) });
        return yield* Effect.logInfo(`removed gVisor ${step.release}`);
      }
      case "InstallStargz": {
        const entries = tarEntries(gunzipSync(yield* verified(step.archive)));
        for (const entry of entries.filter(({ name }) => STARGZ_EXECUTABLES.some((executable) => name === executable))) {
          yield* writeFile(under(root, `${BIN}/${entry.name}`), entry.content, entry.mode);
        }
        yield* recordStamp({ stargz: step.version });
        return yield* Effect.logInfo(`installed stargz-snapshotter ${step.version}`);
      }
      case "InstallK3s": {
        yield* writeFile(under(root, `${BIN}/k3s`), yield* verified(step.binary), 0o755);
        const script = yield* verified(step.script);
        yield* Effect.scoped(Effect.gen(function*() {
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "alasio-k3s-" });
          yield* fs.writeFile(`${directory}/install.sh`, script, { mode: 0o700 });
          // Its binary is there already, checked; the script makes and starts its service, anew though nothing of it changed.
          yield* system.run("sh", [`${directory}/install.sh`], {
            INSTALL_K3S_SKIP_DOWNLOAD: "true",
            INSTALL_K3S_SKIP_SELINUX_RPM: "true",
            INSTALL_K3S_SELINUX_WARN: "true",
            INSTALL_K3S_FORCE_RESTART: "true",
            INSTALL_K3S_EXEC: step.role,
          });
        }));
        yield* recordStamp({ k3s: step.version });
        return yield* Effect.logInfo(`installed k3s ${step.version}`);
      }
      case "StartK3s":
        return yield* (step.restart ? systemctl("restart", SERVICE) : systemctl("enable", "--now", SERVICE));
      case "StopK3s": {
        yield* systemctl("disable", "--now", SERVICE);
        yield* killGvisor(yield* installedGvisor);
        if (yield* fs.exists(under(root, KILLALL))) yield* system.run(KILLALL, []);
        return;
      }
      case "RemoveNode": {
        // k3s first, which would start the pods gVisor's end again.
        if (yield* serviceInstalled) yield* systemctl("stop", SERVICE);
        yield* killGvisor(yield* installedGvisor);
        if (step.firewall) yield* closeFirewall(step.firewall);
        if (yield* fs.exists(under(root, UNINSTALL))) yield* system.run(UNINSTALL, []);
        // stargz-snapshotter's daemon stopped with k3s; its FUSE manager, which keeps its mounts, stops now.
        yield* system.kill([`${BIN}/stargz-fuse-manager`]);
        yield* removeMounted([NODE_DIRECTORY, STARGZ_ROOT]);
        // alasio's own, and the node's password, which k3s keeps and its uninstall script leaves.
        const owned = [
          GVISOR_RELEASES,
          ...STARGZ_EXECUTABLES.map((name) => `${BIN}/${name}`),
          CONFIG_YAML,
          REGISTRIES_YAML,
          STAMP,
          CONTAINERD,
          NODE_PASSWORD,
          STARGZ_UNIT,
          dirname(K3S_DROP_IN),
          dirname(STARGZ_CONFIG),
          // Its FUSE manager's socket among them: the daemon starts no manager where that is, and fails to reach one.
          dirname(STARGZ_SOCKET),
        ];
        for (const path of owned) yield* fs.remove(under(root, path), { recursive: true, force: true });
        yield* systemctl("daemon-reload");
        // What k3s's uninstall script leaves of the directories it made, and of alasio's, when it is empty.
        for (const path of [K3S_ETC, "/etc/rancher", "/var/lib/rancher/k3s", "/var/lib/rancher", dirname(GVISOR_RELEASES)]) {
          const at = under(root, path);
          if ((yield* fs.exists(at)) && (yield* fs.readDirectory(at)).length === 0) yield* fs.remove(at, { recursive: true });
        }
        return yield* Effect.logInfo("removed k3s, gVisor and stargz-snapshotter");
      }
      case "RemoveStorage":
        // A path of the operator's, which they made, as the rest of their own paths.
        yield* fs.remove(step.path, { recursive: true, force: true });
        return yield* Effect.logInfo(`removed ${step.path}`);
      case "ReadKubeconfig": {
        if (step.renew) yield* systemctl("restart", SERVICE);
        return yield* kubeconfigWritten.pipe(
          Effect.retry({ while: (error) => error._tag === "NotYet", schedule: Schedule.max([Schedule.spaced("1 second"), Schedule.during(Duration.minutes(5))]) }),
        );
      }
    }
  });
