function elapsedMs(start) {
    return Number(process.hrtime.bigint() - start) / 1_000_000;
}

function formatTimingExtras(extras) {
    return Object.entries(extras)
        .filter(([, value]) => value !== undefined && value !== null)
        .map(([key, value]) => `${key}=${JSON.stringify(value)}`)
        .join(" ");
}

export { elapsedMs };

export function createTurnTimer({ threadKey, resumeSession, prompt, log }) {
    const startedAt = process.hrtime.bigint();
    let previousAt = startedAt;
    return (label, extras = {}) => {
        const now = process.hrtime.bigint();
        const totalMs = elapsedMs(startedAt).toFixed(1);
        const stepMs = (Number(now - previousAt) / 1_000_000).toFixed(1);
        previousAt = now;
        const session = resumeSession ? resumeSession.slice(0, 8) : "new";
        const renderedExtras = formatTimingExtras(extras);
        log.info(`turn_timing label=${label} total_ms=${totalMs} step_ms=${stepMs} thread_key=${JSON.stringify(threadKey)} resume=${Boolean(resumeSession)} session=${JSON.stringify(session)} prompt_chars=${prompt.length}${renderedExtras ? ` ${renderedExtras}` : ""}`);
    };
}
