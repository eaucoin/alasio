/**
 * The templates the deployment renders for alasio's workspaces: `ALASIO_KUBE_TEMPLATES` is
 * the path of a JSON file, which the Helm chart mounts from a ConfigMap, of the form
 *
 *     {
 *       "sessions": { "namespace", "port", "workspaceDir", "egressGate", "fullModeNameservers",
 *                     "podTemplate", "volumeClaimTemplates" },
 *       "host":     { "namespace", "port", "stateRoot", "podTemplate" }
 *     }
 *
 * either of which may be absent: no `sessions`, no session filesystems; no `host`, no
 * folder workspaces. Each `podTemplate` has a container named `bayma` whose arguments
 * serve MCP over HTTP on `port`; alasio adds the rest per Sandbox (./sandboxes.ts).
 */
import { readFileSync } from "node:fs";

import type { V1PersistentVolumeClaim, V1PodTemplateSpec } from "@kubernetes/client-node";
import { Config, Effect, Predicate, Result, Schema, SchemaGetter, SchemaIssue } from "effect";

/** The deployment's templates could not be read, or are not what alasio needs; the message says which. */
export class KubeTemplatesError extends Schema.TaggedError<KubeTemplatesError>()("KubeTemplatesError", {
  message: Schema.String,
}) {}

const NAMESPACE = /^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$/u;

/**
 * `schema`, whose every failure, a missing key's too, reads as `message`, which the
 * error then says of the key's path.
 */
const said = <S extends Schema.Top>(schema: S, message: string): S["Rebuild"] =>
  schema.annotate({ message }).annotateKey({ messageMissingKey: message });

/** A namespace's name; a name YAML reads as a number (`123`) is that name. */
const Namespace = said(
  Schema.Union([Schema.String, Schema.Finite.pipe(Schema.decodeTo(Schema.String, { decode: SchemaGetter.String(), encode: SchemaGetter.Number() }))])
    .check(Schema.isPattern(NAMESPACE, { message: "must be a namespace name" })),
  "must be a namespace name",
);

const Port = said(
  Schema.Number.check(
    Schema.isInt({ message: "must be a port number" }),
    Schema.isBetween({ minimum: 1, maximum: 65535 }, { message: "must be a port number" }),
  ),
  "must be a port number",
);

const Path = said(Schema.String, "must be a path");

/**
 * What alasio relies on of a pod template is checked with its profile (a container named
 * `bayma`); the rest of it, and the claim templates, are Kubernetes objects the chart
 * renders, which the API server checks as each Sandbox is made.
 */
const PodTemplate = said(Schema.declare((value): value is V1PodTemplateSpec => Predicate.isObject(value)), "must be an object").pipe(
  Schema.withDecodingDefaultKey(Effect.succeed({})),
);

const VolumeClaimTemplates = said(
  Schema.Array(Schema.declare((value): value is V1PersistentVolumeClaim => Predicate.isObject(value))),
  "must be a list of volume claim templates",
);

/** Whether `podTemplate`, as the chart rendered it, runs a container named `bayma`. */
function runsBayma(podTemplate: unknown): boolean {
  const containers = Predicate.hasProperty(podTemplate, "spec") && Predicate.hasProperty(podTemplate.spec, "containers")
    ? podTemplate.spec.containers
    : undefined;
  return Array.isArray(containers) && containers.some((container: unknown) => Predicate.hasProperty(container, "name") && container.name === "bayma");
}

/** What both profiles hold: where their Sandboxes are made, and from what. */
const sandboxProfile = { namespace: Namespace, port: Port, podTemplate: PodTemplate };

/** The issue of a profile whose pod template runs no bayma. */
const baymaIssue = { path: ["podTemplate", "spec", "containers"], issue: 'must include one named "bayma"' };

/** The template of session filesystems' Sandboxes (charts/alasio/templates/alasio/sandbox-templates.yaml). */
const SessionsProfile = said(
  Schema.Struct({
    ...sandboxProfile,
    /** Where the session's workspace is in bayma's container. */
    workspaceDir: Path,
    /** Whether a session waits for its egress to be confined before it starts; only false turns it off. */
    egressGate: Schema.optional(said(Schema.NullOr(Schema.Boolean), "must be true or false")),
    /** The resolvers sessions with internet access use, or none for alasio's defaults. */
    fullModeNameservers: Schema.optional(said(Schema.NullOr(Schema.Array(Schema.String)), "must be a list of addresses")),
    volumeClaimTemplates: Schema.optional(VolumeClaimTemplates),
  }),
  "must be an object",
).check(Schema.makeFilter((profile) => (runsBayma(profile.podTemplate) ? undefined : baymaIssue)));

/** The template of folder workspaces' bayma (charts/alasio/templates/alasio/sandbox-templates.yaml). */
const HostProfile = said(
  Schema.Struct({
    ...sandboxProfile,
    /** The directory on the host under which each conversation's bayma keeps its state. */
    stateRoot: Path,
  }),
  "must be an object",
).check(Schema.makeFilter((profile) => (runsBayma(profile.podTemplate) ? undefined : baymaIssue)));

/** A profile the deployment may leave out, which is then null. */
const optionalProfile = <S extends Schema.Top>(schema: S) =>
  said(Schema.NullOr(schema), "must be an object").pipe(Schema.withDecodingDefaultKey(Effect.succeed(null)));

/** ALASIO_KUBE_TEMPLATES, checked: a profile the deployment does not render is null. */
const KubeTemplates = said(Schema.Struct({ sessions: optionalProfile(SessionsProfile), host: optionalProfile(HostProfile) }), "must be an object");

export type SessionsProfile = typeof SessionsProfile.Type;
export type HostProfile = typeof HostProfile.Type;
export type KubeTemplates = typeof KubeTemplates.Type;

const formatIssue = SchemaIssue.makeFormatterStandardSchemaV1();

/** The first thing wrong with the templates, as alasio says it: the key's path, then what it must be. */
function describe(issue: SchemaIssue.Issue): string {
  const [first] = formatIssue(issue).issues;
  const path = first?.path?.map((segment) => String(Predicate.hasProperty(segment, "key") ? segment.key : segment)).join(".") ?? "";
  return ["ALASIO_KUBE_TEMPLATES", path, first?.message ?? "is not what alasio expects"].filter(Boolean).join(" ");
}

/** The templates in `text`, the content of the file at `path`, checked. */
export const decodeKubeTemplates = (path: string, text: string): Result.Result<KubeTemplates, KubeTemplatesError> =>
  Result.try({
    try: (): unknown => JSON.parse(text),
    catch: (error) => new KubeTemplatesError({ message: `ALASIO_KUBE_TEMPLATES ${path} is not readable JSON: ${error instanceof Error ? error.message : String(error)}` }),
  }).pipe(
    Result.flatMap((parsed) =>
      Result.mapError(Schema.decodeUnknownResult(KubeTemplates)(parsed), (error) => new KubeTemplatesError({ message: describe(error.issue) }))
    ),
  );

/**
 * The templates, read once at startup and checked, so a deployment that renders them
 * wrongly fails as it starts rather than at a workspace's first turn.
 */
export const loadKubeTemplates: Effect.Effect<KubeTemplates, KubeTemplatesError | Config.ConfigError> = Effect.gen(function*() {
  const path = (yield* Config.String("ALASIO_KUBE_TEMPLATES").pipe(Config.withDefault(""))).trim();
  if (!path) return yield* new KubeTemplatesError({ message: "ALASIO_KUBE_TEMPLATES is not set: alasio runs where its Helm chart deploys it" });
  const text = yield* Effect.try({
    try: () => readFileSync(path, "utf8"),
    catch: (error) => new KubeTemplatesError({ message: `ALASIO_KUBE_TEMPLATES ${path} is not readable JSON: ${error instanceof Error ? error.message : String(error)}` }),
  });
  return yield* Effect.fromResult(decodeKubeTemplates(path, text));
});
