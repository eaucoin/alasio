/**
 * How long an alasio is given to stop, as its Deployment says (cli/src/manifests/alasio.ts) and
 * its drain keeps to (./drain.ts). Free of anything but itself, so the command line takes it
 * without the app.
 */

/**
 * How long an alasio is given to stop, in seconds: past the longest Telegram is waited for
 * before a reply is sent again (./outbox.ts, five minutes), so a reply held off as it is told to
 * stop is delivered still.
 */
export const STOP_GRACE_SECONDS = 360;
