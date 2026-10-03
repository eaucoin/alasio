import { query, type Query, type SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import { Effect } from "effect";

import type { ModelOption } from "../index.ts";
import { buildClaudeEnv } from "./env.ts";
import { ClaudeCodeError, type ClaudeQueryParams } from "./runtime.ts";

/** What listClaudeModels is given. */
export interface ClaudeModelListOptions {
  readonly workingDirectory: string;
  /** Starts the query the list is asked through: the SDK's `query`, or a test's stand-in. */
  readonly queryFactory?: (params: ClaudeQueryParams) => Pick<Query, "supportedModels" | "close">;
}

/** A prompt stream that never yields: the list needs a live process, not a turn. */
const idle: AsyncIterable<SDKUserMessage> = {
  async *[Symbol.asyncIterator]() {
    await new Promise(() => {});
  },
};

/**
 * The models this machine's Claude login can use, in the shape /model shows.
 *
 * `supportedModels()` is a control request, so it needs a live CLI process but
 * not a turn: the prompt stream never yields, and the process is closed as soon
 * as the list arrives.
 */
export const listClaudeModels = ({ workingDirectory, queryFactory = query }: ClaudeModelListOptions): Effect.Effect<ModelOption[], ClaudeCodeError> =>
  Effect.acquireUseRelease(
    Effect.try({
      try: () => {
        const controller = new AbortController();
        const process = queryFactory({
          prompt: idle,
          options: {
            cwd: workingDirectory,
            env: buildClaudeEnv(),
            abortController: controller,
            permissionMode: "bypassPermissions",
            allowDangerouslySkipPermissions: true,
            persistSession: false,
          },
        });
        return { controller, process };
      },
      catch: (cause) => new ClaudeCodeError({ cause }),
    }),
    ({ process }) =>
      Effect.tryPromise({ try: () => process.supportedModels(), catch: (cause) => new ClaudeCodeError({ cause }) }).pipe(
        Effect.map((models) =>
          models.map((model) => ({
            id: model.value,
            label: model.displayName ?? model.value,
            description: model.description ?? "",
            resolvedModel: model.resolvedModel ?? model.value,
            efforts: model.supportsEffort ? [...(model.supportedEffortLevels ?? [])] : [],
            defaultEffort: null,
            isDefault: model.value === "default",
          }))
        ),
      ),
    ({ controller, process }) =>
      Effect.sync(() => {
        controller.abort();
        try {
          process.close?.();
        } catch {
          // Already closed.
        }
      }),
  );
