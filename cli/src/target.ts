/**
 * The cluster the operator's config targets (./config.ts): one alasio makes on this
 * machine, k3s on the machine itself or in Docker, whose kubeconfig it writes beside the
 * config file, or one a kubeconfig reaches; and the services that reach it.
 *
 * The nodes in Docker mount the host profile's paths at their own, as folder workspaces
 * mount them from the node, but for those the target's own mounts already put there (as
 * a node path of /host/tmp mounts the machine's /tmp). k3s on the machine itself has
 * them where they are.
 */
import { Config, Effect, FileSystem, Layer, Option, Result } from "effect";

import { DockerEngine, type DockerHostUnsupported } from "./cluster/docker-engine.ts";
import { DockerCluster, type DockerClusterOptions } from "./cluster/docker.ts";
import type { HostClusterOptions } from "./cluster/host.ts";
import { clusterKubeconfigPath, defaultStoragePath, installConfigOf, type OperatorConfig, OperatorConfigError } from "./config.ts";
import { KubeApi, type KubeconfigRef, type KubeconfigUnusable } from "./kube/api.ts";

/** The cluster a config targets: one alasio makes, on this machine or in Docker, with what it is made with, or one a kubeconfig reaches. */
export type ResolvedTarget =
  | { readonly _tag: "Host"; readonly cluster: HostClusterOptions; readonly kubeconfig: { readonly path: string } }
  | { readonly _tag: "Docker"; readonly cluster: DockerClusterOptions; readonly kubeconfig: { readonly path: string } }
  | { readonly _tag: "Kubeconfig"; readonly kubeconfig: KubeconfigRef };

/** The cluster `config` targets. */
export const resolveTarget = Effect.fnUntraced(function*(config: OperatorConfig): Effect.fn.Return<ResolvedTarget, Config.ConfigError | OperatorConfigError> {
  const { target } = config;
  if ("kubeconfig" in target) return { _tag: "Kubeconfig", kubeconfig: target.kubeconfig };
  const kubeconfig = { path: clusterKubeconfigPath(config) };
  if ("host" in target) {
    const { host } = target;
    return {
      _tag: "Host",
      cluster: {
        storagePath: host.storagePath ?? (yield* defaultStoragePath),
        hostAliases: host.hostAliases,
        ...(host.registries ? { registries: host.registries } : {}),
      },
      kubeconfig,
    };
  }
  const { docker } = target;
  const install = installConfigOf(config.install, { claude: false });
  if (Result.isFailure(install)) return yield* new OperatorConfigError({ path: config.path, reason: install.failure });
  const hostProfile = install.success.host;
  const hostPaths = hostProfile.enabled ? hostProfile.mounts.map(({ hostPath, readOnly }) => ({ source: hostPath, target: hostPath, readOnly })) : [];
  return {
    _tag: "Docker",
    cluster: {
      name: docker.name,
      apiPort: docker.apiPort,
      storagePath: docker.storagePath ?? (yield* defaultStoragePath),
      ...(docker.subnet ? { subnet: docker.subnet } : {}),
      ...(docker.image ? { image: docker.image } : {}),
      hostAliases: docker.hostAliases,
      mounts: [...docker.mounts, ...hostPaths.filter(({ target }) => !docker.mounts.some((mount) => mount.target === target))],
      agents: docker.agents,
      ...(docker.registries ? { registries: docker.registries } : {}),
    },
    kubeconfig,
  };
});

/** Docker, as DOCKER_HOST names it. */
const dockerEngine: Layer.Layer<DockerEngine, DockerHostUnsupported | Config.ConfigError> = Layer.unwrap(
  Effect.map(Config.option(Config.String("DOCKER_HOST")), (host) => DockerEngine.layer(Option.match(host, { onNone: () => ({}), onSome: (DOCKER_HOST) => ({ DOCKER_HOST }) }))),
);

/** The cluster in Docker made with `options`. */
export const dockerCluster = (options: DockerClusterOptions): Layer.Layer<DockerCluster, DockerHostUnsupported | Config.ConfigError, FileSystem.FileSystem> =>
  DockerCluster.layer(options).pipe(Layer.provide(dockerEngine));

/** The API of the cluster `target` is. */
export const kubeApi = (target: ResolvedTarget): Layer.Layer<KubeApi, KubeconfigUnusable> => KubeApi.layer(target.kubeconfig);

/** Where the cluster `target` is, as people read it. */
export function describeTarget(target: ResolvedTarget): string {
  if (target._tag === "Host") return "k3s on this machine";
  if (target._tag === "Docker") return `the cluster ${target.cluster.name} on this machine`;
  const { path, context } = target.kubeconfig;
  return `the cluster ${context ? `of context ${context}` : "of the current context"} of ${path ?? "the default kubeconfig"}`;
}
