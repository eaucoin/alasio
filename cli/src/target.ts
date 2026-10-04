/**
 * The cluster the operator's config targets (./config.ts): the one alasio makes on this
 * machine, whose kubeconfig it writes beside the config file, or one a kubeconfig
 * reaches; and the services that reach it.
 *
 * The local cluster's nodes mount the host profile's paths at their own, as folder
 * workspaces mount them from the node.
 */
import { Config, Effect, FileSystem, Layer, Option, Result } from "effect";

import { DockerEngine, type DockerHostUnsupported } from "./cluster/docker.ts";
import { LocalCluster, type LocalClusterOptions } from "./cluster/local.ts";
import { defaultStoragePath, installConfigOf, localKubeconfigPath, type OperatorConfig, OperatorConfigError } from "./config.ts";
import { KubeApi, type KubeconfigRef, type KubeconfigUnusable } from "./kube/api.ts";

/** The cluster a config targets: the local one, with what it is made with, or one a kubeconfig reaches. */
export type ResolvedTarget =
  | { readonly _tag: "Local"; readonly cluster: LocalClusterOptions; readonly kubeconfig: { readonly path: string } }
  | { readonly _tag: "Kubeconfig"; readonly kubeconfig: KubeconfigRef };

/** The cluster `config` targets. */
export const resolveTarget = Effect.fnUntraced(function*(config: OperatorConfig): Effect.fn.Return<ResolvedTarget, Config.ConfigError | OperatorConfigError> {
  const { target } = config;
  if ("kubeconfig" in target) return { _tag: "Kubeconfig", kubeconfig: target.kubeconfig };
  const { local } = target;
  const install = installConfigOf(config.install, { claude: false });
  if (Result.isFailure(install)) return yield* new OperatorConfigError({ path: config.path, reason: install.failure });
  const { host } = install.success;
  const hostPaths = host.enabled ? host.mounts.map(({ hostPath, readOnly }) => ({ source: hostPath, target: hostPath, readOnly })) : [];
  return {
    _tag: "Local",
    cluster: {
      name: local.name,
      apiPort: local.apiPort,
      storagePath: local.storagePath ?? (yield* defaultStoragePath),
      ...(local.subnet ? { subnet: local.subnet } : {}),
      ...(local.image ? { image: local.image } : {}),
      hostAliases: local.hostAliases,
      mounts: [...local.mounts, ...hostPaths.filter(({ source }) => !local.mounts.some((mount) => mount.source === source))],
      agents: local.agents,
    },
    kubeconfig: { path: localKubeconfigPath(config) },
  };
});

/** Docker, as DOCKER_HOST names it. */
const docker: Layer.Layer<DockerEngine, DockerHostUnsupported | Config.ConfigError> = Layer.unwrap(
  Effect.map(Config.option(Config.String("DOCKER_HOST")), (host) => DockerEngine.layer(Option.match(host, { onNone: () => ({}), onSome: (DOCKER_HOST) => ({ DOCKER_HOST }) }))),
);

/** The local cluster made with `options`. */
export const localCluster = (options: LocalClusterOptions): Layer.Layer<LocalCluster, DockerHostUnsupported | Config.ConfigError, FileSystem.FileSystem> =>
  LocalCluster.layer(options).pipe(Layer.provide(docker));

/** The API of the cluster `target` is. */
export const kubeApi = (target: ResolvedTarget): Layer.Layer<KubeApi, KubeconfigUnusable> => KubeApi.layer(target.kubeconfig);

/** Where the cluster `target` is, as people read it. */
export function describeTarget(target: ResolvedTarget): string {
  if (target._tag === "Local") return `the cluster ${target.cluster.name} on this machine`;
  const { path, context } = target.kubeconfig;
  return `the cluster ${context ? `of context ${context}` : "of the current context"} of ${path ?? "the default kubeconfig"}`;
}
