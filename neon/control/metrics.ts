/**
 * neon-control's own metrics, in Prometheus's text format, for the stack's telemetry
 * collector to scrape from each of its replicas (cli/src/manifests/collector.ts): its
 * wakes of the computes, how long each took, warm or from sleep, and those that failed;
 * the computes it put to sleep; how many replicas each compute runs, as the replica that
 * leads last read; how far each branch's read replica trails its primary, while both
 * run; and the locks of its record it gave up waiting for (./record.ts).
 */

/** The bounds, in seconds, of the wake durations' histogram's buckets: a warm wake's first, a cold one's after. */
const WAKE_BUCKETS = [0.05, 0.25, 1, 2.5, 5, 10, 20, 30, 45, 60, 120] as const;

const HELP = {
  neon_control_wakes_total: "Computes woken, or found awake, for a connection or the command line, by how they started: warm, or from sleep (cold)",
  neon_control_wake_duration_seconds: "How long a wake took, from its asking to the compute taking connections, by how it started",
  neon_control_wake_failures_total: "Wakes that failed: the compute did not take connections in time, or could not be scaled",
  neon_control_suspends_total: "Computes put to sleep, idle as long as their environment allows",
  neon_control_compute_replicas: "The replicas each compute's Deployment runs, 0 asleep, as the replica that leads last read them",
  neon_control_lock_timeouts_total: "Locks of the record waited for longer than their patience, by lock: changes to the branches, or a compute's",
  neon_control_replica_lag_bytes: "The WAL a branch's read replica has yet to replay of what its primary has flushed, by the replica's compute, while both run, as the replica that leads last read it",
} as const;

/** How a wake started. */
export type WakeStart = "warm" | "cold";

/** neon-control's metrics: what happened, added to, and rendered for a scrape. */
export interface ControlMetrics {
  woke(start: WakeStart, seconds: number): void;
  wakeFailed(): void;
  suspended(): void;
  /** The replicas of each compute, by its id, replacing what was set before; none for a replica that does not lead. */
  replicas(byCompute: ReadonlyMap<string, number>): void;
  /** The lag of each read replica, by its compute's id, replacing what was set before; none for a replica that does not lead. */
  replicaLag(byCompute: ReadonlyMap<string, number>): void;
  lockTimedOut(lock: "changes" | "compute"): void;
  /** The exposition text, every metric with its HELP and TYPE. */
  render(): string;
}

export function createControlMetrics(): ControlMetrics {
  const wakes = { warm: 0, cold: 0 };
  const durations = { warm: { counts: WAKE_BUCKETS.map(() => 0), count: 0, sum: 0 }, cold: { counts: WAKE_BUCKETS.map(() => 0), count: 0, sum: 0 } };
  let failures = 0;
  let suspends = 0;
  let replicas: ReadonlyMap<string, number> = new Map();
  let lag: ReadonlyMap<string, number> = new Map();
  const lockTimeouts = { changes: 0, compute: 0 };
  const head = (name: keyof typeof HELP, type: string) => [`# HELP ${name} ${HELP[name]}`, `# TYPE ${name} ${type}`];
  return {
    woke(start, seconds) {
      wakes[start] += 1;
      const histogram = durations[start];
      WAKE_BUCKETS.forEach((bound, index) => {
        if (seconds <= bound) histogram.counts[index] = (histogram.counts[index] ?? 0) + 1;
      });
      histogram.count += 1;
      histogram.sum += seconds;
    },
    wakeFailed() {
      failures += 1;
    },
    suspended() {
      suspends += 1;
    },
    replicas(byCompute) {
      replicas = new Map(byCompute);
    },
    replicaLag(byCompute) {
      lag = new Map(byCompute);
    },
    lockTimedOut(lock) {
      lockTimeouts[lock] += 1;
    },
    render() {
      const lines = [
        ...head("neon_control_wakes_total", "counter"),
        ...(["warm", "cold"] as const).map((start) => `neon_control_wakes_total{start="${start}"} ${wakes[start]}`),
        ...head("neon_control_wake_duration_seconds", "histogram"),
        ...(["warm", "cold"] as const).flatMap((start) => {
          const { counts, count, sum } = durations[start];
          return [
            ...WAKE_BUCKETS.map((bound, index) => `neon_control_wake_duration_seconds_bucket{start="${start}",le="${bound}"} ${counts[index] ?? 0}`),
            `neon_control_wake_duration_seconds_bucket{start="${start}",le="+Inf"} ${count}`,
            `neon_control_wake_duration_seconds_sum{start="${start}"} ${sum}`,
            `neon_control_wake_duration_seconds_count{start="${start}"} ${count}`,
          ];
        }),
        ...head("neon_control_wake_failures_total", "counter"),
        `neon_control_wake_failures_total ${failures}`,
        ...head("neon_control_suspends_total", "counter"),
        `neon_control_suspends_total ${suspends}`,
        ...head("neon_control_compute_replicas", "gauge"),
        ...[...replicas].map(([compute, count]) => `neon_control_compute_replicas{compute="${compute}"} ${count}`),
        ...head("neon_control_lock_timeouts_total", "counter"),
        ...(["changes", "compute"] as const).map((lock) => `neon_control_lock_timeouts_total{lock="${lock}"} ${lockTimeouts[lock]}`),
        ...head("neon_control_replica_lag_bytes", "gauge"),
        ...[...lag].map(([compute, bytes]) => `neon_control_replica_lag_bytes{compute="${compute}"} ${bytes}`),
      ];
      return `${lines.join("\n")}\n`;
    },
  };
}
