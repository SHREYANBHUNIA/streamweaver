export type StreamWeaverSnapshot = {
  source: "engine" | "demo";
  status: "running" | "idle" | "unavailable";
  pipelineId: string;
  generatedAt: Date;
  lastError?: string;
  throughputPerSecond: number;
  p95LatencyMs: number;
  queueDepth: number;
  queueCapacity: number;
  watermark: Date;
  checkpoint: {
    pipelineId: string;
    watermark: Date;
    createdAt: Date;
    offsetCount: number;
  };
  metrics: {
    received: number;
    processed: number;
    filtered: number;
    duplicates: number;
    lateDropped: number;
    lateSideOutput: number;
    lateAccumulated: number;
    backpressureRejects: number;
    checkpointCount: number;
    recoveryFailures: number;
    lastLatencyMs: number;
  };
  windows: Array<{
    id: string;
    key: string;
    windowStart: Date;
    windowEnd: Date;
    sum: number;
    count: number;
    emitted: boolean;
  }>;
  lateEvents: Array<{ id: string; key: string; amount: number; eventTime: Date }>;
  alerts: Array<{
    id: string;
    kind: string;
    windowId?: string;
    message: string;
    sum?: number;
    threshold?: number;
    createdAt: Date;
  }>;
  recoveryPolicy: string;
};

const minute = 60_000;

function date(value: string | Date | undefined, fallback: Date): Date {
  if (!value) return fallback;
  const result = new Date(value);
  return Number.isNaN(result.valueOf()) ? fallback : result;
}

export function createDemoSnapshot(now = new Date()): StreamWeaverSnapshot {
  const windowEnd = new Date(Math.floor(now.getTime() / 10_000) * 10_000);
  const windowStart = new Date(windowEnd.getTime() - 10_000);
  const previousEnd = new Date(windowStart.getTime());
  const previousStart = new Date(previousEnd.getTime() - 10_000);

  return {
    source: "demo",
    status: "running",
    pipelineId: "transactions-sum-10s",
    generatedAt: now,
    throughputPerSecond: 1842,
    p95LatencyMs: 38,
    queueDepth: 76,
    queueCapacity: 1024,
    watermark: new Date(now.getTime() - 2_000),
    checkpoint: {
      pipelineId: "transactions-sum-10s",
      watermark: new Date(now.getTime() - 2_000),
      createdAt: new Date(now.getTime() - 5_000),
      offsetCount: 3,
    },
    metrics: {
      received: 2_891_404,
      processed: 2_888_992,
      filtered: 924,
      duplicates: 31,
      lateDropped: 0,
      lateSideOutput: 17,
      lateAccumulated: 0,
      backpressureRejects: 3,
      checkpointCount: 842,
      recoveryFailures: 0,
      lastLatencyMs: 21,
    },
    windows: [
      { id: "current-merchant-orbit", key: "merchant-orbit", windowStart, windowEnd, sum: 12_480, count: 48, emitted: false },
      { id: "previous-merchant-orbit", key: "merchant-orbit", windowStart: previousStart, windowEnd: previousEnd, sum: 8_410, count: 37, emitted: true },
      { id: "previous-merchant-nova", key: "merchant-nova", windowStart: previousStart, windowEnd: previousEnd, sum: 4_326, count: 22, emitted: true },
    ],
    lateEvents: [
      { id: "txn-late-784", key: "merchant-nova", amount: 120, eventTime: new Date(now.getTime() - 14 * minute) },
    ],
    alerts: [
      {
        id: "threshold-current-merchant-orbit",
        kind: "threshold_crossed",
        windowId: "current-merchant-orbit",
        message: "SUM(amount) exceeded the configured alert threshold in a 10-second window.",
        sum: 12_480,
        threshold: 10_000,
        createdAt: new Date(now.getTime() - 8_000),
      },
    ],
    recoveryPolicy: "RocksDB state is durable before latest checkpoint advances; Kafka commits only after checkpoint success; persisted event IDs make source redelivery idempotent.",
  };
}

function adaptEngineSnapshot(payload: Record<string, any>): StreamWeaverSnapshot {
  const now = new Date();
  const metrics = payload.metrics ?? {};
  return {
    source: "engine",
    status: payload.status === "running" ? "running" : "idle",
    pipelineId: payload.pipelineId ?? "transactions-sum-10s",
    generatedAt: now,
    throughputPerSecond: Number(payload.throughputPerSecond ?? 0),
    p95LatencyMs: Number(payload.p95LatencyMs ?? metrics.lastLatency ?? 0),
    queueDepth: Number(payload.queueDepth ?? 0),
    queueCapacity: Number(payload.queueCapacity ?? 1),
    watermark: date(payload.watermark, now),
    checkpoint: {
      pipelineId: payload.checkpoint?.pipelineId ?? payload.pipelineId ?? "transactions-sum-10s",
      watermark: date(payload.checkpoint?.watermark, now),
      createdAt: date(payload.checkpoint?.createdAt, now),
      offsetCount: Object.keys(payload.checkpoint?.offsets ?? {}).length,
    },
    metrics: {
      received: Number(metrics.received ?? 0),
      processed: Number(metrics.processed ?? 0),
      filtered: Number(metrics.filtered ?? 0),
      duplicates: Number(metrics.duplicates ?? 0),
      lateDropped: Number(metrics.lateDropped ?? 0),
      lateSideOutput: Number(metrics.lateSideOutput ?? 0),
      lateAccumulated: Number(metrics.lateAccumulated ?? 0),
      backpressureRejects: Number(metrics.backpressureRejects ?? 0),
      checkpointCount: Number(metrics.checkpointCount ?? 0),
      recoveryFailures: Number(metrics.recoveryFailures ?? 0),
      lastLatencyMs: Number(metrics.lastLatency ?? 0) / 1_000_000,
    },
    windows: (payload.windows ?? []).map((entry: Record<string, any>) => ({
      id: String(entry.id), key: String(entry.key), windowStart: date(entry.windowStart, now), windowEnd: date(entry.windowEnd, now), sum: Number(entry.sum ?? 0), count: Number(entry.count ?? 0), emitted: Boolean(entry.emitted),
    })),
    lateEvents: (payload.lateEvents ?? []).map((entry: Record<string, any>) => ({
      id: String(entry.id), key: String(entry.key), amount: Number(entry.amount ?? 0), eventTime: date(entry.eventTime, now),
    })),
    alerts: (payload.alerts ?? []).map((entry: Record<string, any>) => ({
      id: String(entry.id), kind: String(entry.kind), windowId: entry.windowId ? String(entry.windowId) : undefined, message: String(entry.message), sum: entry.sum === undefined ? undefined : Number(entry.sum), threshold: entry.threshold === undefined ? undefined : Number(entry.threshold), createdAt: date(entry.createdAt, now),
    })),
    recoveryPolicy: String(payload.recoveryPolicy ?? "The engine did not report its recovery policy."),
  };
}

export async function getStreamWeaverSnapshot(endpoint = process.env.STREAMWEAVER_API_URL): Promise<StreamWeaverSnapshot> {
  if (!endpoint) return createDemoSnapshot();
  try {
    const response = await fetch(`${endpoint.replace(/\/$/, "")}/v1/status`, { signal: AbortSignal.timeout(1_500) });
    if (!response.ok) throw new Error(`Go API returned HTTP ${response.status}`);
    return adaptEngineSnapshot(await response.json());
  } catch (error) {
    const fallback = createDemoSnapshot();
    return { ...fallback, status: "unavailable", lastError: error instanceof Error ? error.message : "Could not reach Go API" };
  }
}
