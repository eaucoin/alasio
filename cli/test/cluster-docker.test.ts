import assert from "node:assert/strict";
import { Readable } from "node:stream";
import { test } from "node:test";

import { Effect, Result } from "effect";

import { demultiplex, DockerEngine, DockerError, dockerSocket, firstFile } from "../src/cluster/docker.ts";
import { type FakeDocker, type FakeDockerOptions, frame, serveFakeDocker, tarOf } from "./support/fake-docker.ts";

/** Runs `body` with the client on a fake Docker served for it. */
async function withDocker<A>(
  body: (docker: DockerEngine["Service"], fake: FakeDocker) => Effect.Effect<A, unknown>,
  options: FakeDockerOptions = {},
): Promise<A> {
  const fake = await serveFakeDocker(options);
  try {
    return await Effect.runPromise(
      DockerEngine.pipe(Effect.flatMap((docker) => body(docker, fake)), Effect.provide(DockerEngine.layer({ DOCKER_HOST: fake.host }))),
    );
  } finally {
    await fake.close();
  }
}

const container = { Image: "node", Hostname: "n", Cmd: ["server"], Env: [], Labels: { "alasio.cluster": "c" } } as const;
const hostConfig = {
  Privileged: true,
  Init: true,
  CgroupnsMode: "private",
  RestartPolicy: { Name: "unless-stopped" },
  SecurityOpt: [],
  Tmpfs: {},
  Mounts: [],
  ExtraHosts: [],
} as const;
const containerCreate = { ...container, HostConfig: hostConfig, NetworkingConfig: { EndpointsConfig: {} } };

test("DOCKER_HOST names the socket, Docker's own unless set, and only a unix:// one", () => {
  assert.deepEqual(dockerSocket({}), Result.succeed("/var/run/docker.sock"));
  assert.deepEqual(dockerSocket({ DOCKER_HOST: "unix:///run/user/1000/docker.sock" }), Result.succeed("/run/user/1000/docker.sock"));
  const refused = dockerSocket({ DOCKER_HOST: "tcp://10.0.0.1:2375" });
  assert.ok(Result.isFailure(refused));
  assert.match(refused.failure.message, /tcp:\/\/10\.0\.0\.1:2375.*Unix socket/u);
});

test("an object that is not there is null, and one that is, Docker's answer", async () => {
  const seen = await withDocker((docker, fake) =>
    Effect.gen(function*() {
      const before = yield* docker.inspectContainer("n");
      yield* docker.createContainer("n", containerCreate);
      fake.images.add("node:1");
      return { before, after: yield* docker.inspectContainer("n"), image: yield* docker.inspectImage("node:1"), network: yield* docker.inspectNetwork("c") };
    })
  );
  assert.equal(seen.before, null);
  assert.deepEqual(seen.after?.State, { Status: "created", Running: false, StartedAt: "0001-01-01T00:00:00Z" });
  assert.deepEqual(seen.after?.Config.Labels, { "alasio.cluster": "c" });
  assert.equal(seen.image?.Id, "sha256:node:1");
  assert.equal(seen.network, null);
});

test("calls go to API version 1.44, with their names in the query and their bodies as JSON", async () => {
  const requests = await withDocker((docker, fake) =>
    Effect.gen(function*() {
      yield* docker.createContainer("n", containerCreate);
      yield* docker.listContainers({ "alasio.cluster": "c" });
      return fake.requests;
    })
  );
  const [create, list] = requests;
  assert.equal(create?.method, "POST");
  assert.equal(create?.path, "/containers/create");
  assert.equal(create?.query.get("name"), "n");
  assert.deepEqual(create?.body, containerCreate);
  assert.equal(list?.query.get("all"), "true");
  assert.deepEqual(JSON.parse(list?.query.get("filters") ?? ""), { label: ["alasio.cluster=c"] });
});

test("a refusal carries Docker's status and message", async () => {
  const error = await withDocker((docker) =>
    docker.createContainer("n", containerCreate).pipe(Effect.andThen(docker.createContainer("n", containerCreate)), Effect.flip)
  );
  assert.ok(error instanceof DockerError);
  assert.equal(error.status, 409);
  assert.equal(error.call, "POST /containers/create");
  assert.match(error.message, /^Docker answered 409 to POST \/containers\/create: Conflict\. The container name "\/n" is already in use$/u);
});

test("starting a running container, stopping a stopped one and removing a removed one are not errors", async () => {
  const changes = await withDocker((docker, fake) =>
    Effect.gen(function*() {
      yield* docker.createContainer("n", containerCreate);
      yield* docker.startContainer("n");
      yield* docker.startContainer("n");
      yield* docker.stopContainer("n");
      yield* docker.stopContainer("n");
      yield* docker.removeContainer("n");
      yield* docker.removeContainer("n");
      yield* docker.removeNetwork("c");
      yield* docker.removeVolume("v");
      return fake.changes();
    })
  );
  assert.equal(changes.length, 9);
});

test("a pull that fails after Docker answered 200 fails with the error its progress ends with", async () => {
  const error = await withDocker(
    (docker, fake) =>
      docker.pullImage("ghcr.io/eaucoin/alasio-node:1").pipe(
        Effect.andThen(Effect.sync(() => assert.ok(fake.images.has("ghcr.io/eaucoin/alasio-node:1")))),
        Effect.andThen(docker.pullImage("ghcr.io/eaucoin/private:1")),
        Effect.flip,
      ),
    { pullable: (reference) => !reference.includes("private") },
  );
  assert.equal(error.reason, "denied");
  assert.equal(error.status, undefined);
});

test("volumes are made once and listed by label", async () => {
  const volumes = await withDocker((docker) =>
    Effect.gen(function*() {
      yield* docker.createVolume({ Name: "a", Labels: { "alasio.cluster": "c" } });
      yield* docker.createVolume({ Name: "a", Labels: { "alasio.cluster": "c" } });
      yield* docker.createVolume({ Name: "b", Labels: { "alasio.cluster": "other" } });
      return yield* docker.listVolumes({ "alasio.cluster": "c" });
    })
  );
  assert.deepEqual(volumes, [{ Name: "a", Labels: { "alasio.cluster": "c" } }]);
});

test("exec runs the command, sends its stdin, and parts its output from its errors", async () => {
  const runs: { container: string; command: readonly string[]; stdin: string }[] = [];
  const results = await withDocker(
    (docker, fake) =>
      Effect.gen(function*() {
        yield* docker.createContainer("n", containerCreate);
        yield* docker.startContainer("n");
        const plain = yield* docker.exec("n", ["kubectl", "get", "nodes"]);
        const fed = yield* docker.exec("n", ["kubectl", "apply", "--filename=-"], { stdin: "{\"kind\":\"RuntimeClass\"}" });
        const start = fake.requests.find(({ path }) => path === "/exec/exec-0/start");
        return { plain, fed, startBody: start?.body };
      }),
    {
      onExec: (container, command, stdin) => {
        runs.push({ container, command, stdin: stdin.toString("utf8") });
        return command.includes("apply") ? { exitCode: 1, stderr: "refused" } : { exitCode: 0, stdout: "node Ready\n", stderr: "warning\n" };
      },
    },
  );
  assert.deepEqual(runs, [
    { container: "n", command: ["kubectl", "get", "nodes"], stdin: "" },
    { container: "n", command: ["kubectl", "apply", "--filename=-"], stdin: "{\"kind\":\"RuntimeClass\"}" },
  ]);
  assert.deepEqual(results.startBody, { Detach: false, Tty: false });
  assert.equal(results.plain.exitCode, 0);
  assert.equal(results.plain.stdout.toString("utf8"), "node Ready\n");
  assert.equal(results.plain.stderr, "warning\n");
  assert.deepEqual({ ...results.fed, stdout: results.fed.stdout.toString("utf8") }, { exitCode: 1, stdout: "", stderr: "refused" });
});

test("an image export streams into an exec's stdin", async () => {
  let imported = "";
  await withDocker(
    (docker) =>
      Effect.gen(function*() {
        yield* docker.createContainer("n", containerCreate);
        yield* docker.startContainer("n");
        yield* Effect.scoped(
          Effect.flatMap(docker.exportImages(["alasio:e2e", "alasio-agent:e2e"]), (archive) => docker.exec("n", ["ctr", "images", "import", "-"], { stdin: archive })),
        );
      }),
    {
      onExec: (_container, _command, stdin) => {
        imported = stdin.toString("utf8");
        return { exitCode: 0 };
      },
    },
  );
  assert.equal(imported, "archive of alasio:e2e alasio-agent:e2e");
});

test("a stream fed to an exec that fails fails the exec", async () => {
  const failing = new Readable({ read() { this.destroy(new Error("export broke")); } });
  const error = await withDocker((docker) =>
    Effect.gen(function*() {
      yield* docker.createContainer("n", containerCreate);
      yield* docker.startContainer("n");
      return yield* Effect.flip(docker.exec("n", ["ctr", "images", "import", "-"], { stdin: failing }));
    })
  );
  assert.match(error.message, /export broke/u);
});

test("exec in a container that is not running is Docker's refusal", async () => {
  const error = await withDocker((docker) =>
    docker.createContainer("n", containerCreate).pipe(Effect.andThen(docker.exec("n", ["true"])), Effect.flip)
  );
  assert.equal(error.status, 409);
});

test("readFile reads the file out of the archive Docker sends, and a missing one is a 404", async () => {
  const results = await withDocker(
    (docker) =>
      Effect.gen(function*() {
        yield* docker.createContainer("n", containerCreate);
        return { token: yield* docker.readFile("n", "/var/lib/rancher/k3s/server/token"), missing: yield* Effect.flip(docker.readFile("n", "/nothing")) };
      }),
    { files: (_container, path) => (path.endsWith("/token") ? Buffer.from("K10abc::server:secret\n") : null) },
  );
  assert.equal(results.token.toString("utf8"), "K10abc::server:secret\n");
  assert.equal(results.missing.status, 404);
});

test("a Docker that cannot be reached fails with why, and no status", async () => {
  const error = await Effect.runPromise(
    DockerEngine.pipe(Effect.flatMap((docker) => Effect.flip(docker.version)), Effect.provide(DockerEngine.layer({ DOCKER_HOST: "unix:///nonexistent/docker.sock" }))),
  );
  assert.equal(error.status, undefined);
  assert.match(error.message, /^Docker failed GET \/version: .*ENOENT/u);
});

test("firstFile reads past entries that are not regular files", () => {
  const pax = tarOf("PaxHeaders/k3s.yaml", Buffer.from("30 mtime=1700000000.000000000\n"));
  pax[156] = "x".charCodeAt(0);
  const file = tarOf("k3s.yaml", Buffer.from("apiVersion: v1\n"));
  assert.equal(firstFile(Buffer.concat([pax.subarray(0, 1024), file]))?.toString("utf8"), "apiVersion: v1\n");
  assert.equal(firstFile(Buffer.alloc(1024)), null);
});

test("demultiplex parts frames by stream, in order", () => {
  const { stdout, stderr } = demultiplex(Buffer.concat([frame(1, "a"), frame(2, "b"), frame(1, "c")]));
  assert.equal(stdout.toString("utf8"), "ac");
  assert.equal(stderr.toString("utf8"), "b");
});
