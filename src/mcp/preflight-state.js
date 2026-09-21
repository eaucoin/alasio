import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { dirname } from "node:path";

const STATE_DIR_FLAG = "--state-dir";

export async function isolatePreflightState(serverConfig) {
  const args = Array.isArray(serverConfig?.args)
    ? serverConfig.args.map((value) => String(value))
    : [];
  const stateDirIndex = args.indexOf(STATE_DIR_FLAG);
  const runtimeStateDir = stateDirIndex >= 0 ? args[stateDirIndex + 1] : null;
  if (!runtimeStateDir) {
    return {
      serverConfig,
      cleanup: async () => {},
    };
  }

  await mkdir(dirname(runtimeStateDir), { recursive: true });
  const preflightStateDir = await mkdtemp(`${runtimeStateDir}-preflight-`);
  args[stateDirIndex + 1] = preflightStateDir;

  return {
    serverConfig: {
      ...serverConfig,
      args,
    },
    cleanup: async () => {
      await rm(preflightStateDir, { recursive: true, force: true });
    },
  };
}
