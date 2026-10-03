import type { HarnessName } from "../harness/names.ts";
import type { Logger } from "../shared/log.ts";
import { currentSpan, meter } from "../telemetry/index.ts";

/** A value a turn's timeline records with a label; an object is recorded as its JSON. */
export type TurnTimingValue = string | number | boolean | object | null | undefined;

/** Records a label on a turn's timeline, with values of its own. */
export type TurnTimer = (label: string, extras?: Readonly<Record<string, TurnTimingValue>>) => void;

export interface TurnTimerOptions {
    readonly harness: HarnessName;
    readonly threadKey: string;
    readonly resumeSession: string | null | undefined;
    readonly prompt: string;
    readonly log: Logger;
}

const firstOutput = meter.createHistogram("alasio.turn.first_output", {
    description: "Time from a turn reaching its harness to the first output the harness shows, by harness",
    unit: "s",
});

function elapsedMs(start: bigint): number {
    return Number(process.hrtime.bigint() - start) / 1_000_000;
}

function definedExtras(extras: Readonly<Record<string, TurnTimingValue>>) {
    return Object.entries(extras).filter((entry): entry is [string, string | number | boolean | object] => entry[1] !== undefined && entry[1] !== null);
}

export { elapsedMs };

/**
 * The timeline of one turn in its harness: each label is logged with the time since the
 * turn reached the harness and since the previous label, and recorded as an event of the
 * turn's span, taken here because a harness reports some labels from outside it. The
 * first visible output is also measured.
 */
export function createTurnTimer({ harness, threadKey, resumeSession, prompt, log }: TurnTimerOptions): TurnTimer {
    const startedAt = process.hrtime.bigint();
    const span = currentSpan();
    let previousAt = startedAt;
    return (label, extras = {}) => {
        const now = process.hrtime.bigint();
        const totalMs = elapsedMs(startedAt);
        const stepMs = (Number(now - previousAt) / 1_000_000).toFixed(1);
        previousAt = now;
        const session = resumeSession ? resumeSession.slice(0, 8) : "new";
        const defined = definedExtras(extras);
        span.addEvent(label, Object.fromEntries(defined.map(([key, value]) => [key, typeof value === "object" ? JSON.stringify(value) : value])));
        if (label === "first_visible_item") {
            firstOutput.record(totalMs / 1000, { "alasio.harness": harness });
        }
        const renderedExtras = defined.map(([key, value]) => `${key}=${JSON.stringify(value)}`).join(" ");
        log.info(`turn_timing label=${label} total_ms=${totalMs.toFixed(1)} step_ms=${stepMs} thread_key=${JSON.stringify(threadKey)} resume=${Boolean(resumeSession)} session=${JSON.stringify(session)} prompt_chars=${prompt.length}${renderedExtras ? ` ${renderedExtras}` : ""}`);
    };
}
