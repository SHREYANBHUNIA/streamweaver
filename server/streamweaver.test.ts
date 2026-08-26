import { describe, expect, it } from "vitest";
import { createDemoSnapshot, getStreamWeaverSnapshot } from "./streamweaver";

describe("StreamWeaver telemetry gateway", () => {
  it("produces a clearly marked local demo snapshot when no engine endpoint is configured", async () => {
    const snapshot = await getStreamWeaverSnapshot("");
    expect(snapshot.source).toBe("demo");
    expect(snapshot.pipelineId).toBe("transactions-sum-10s");
    expect(snapshot.alerts[0]?.message).toContain("SUM(amount)");
  });

  it("keeps the transaction demo wording and 10-second aggregate in the telemetry contract", () => {
    const snapshot = createDemoSnapshot(new Date("2026-08-26T12:00:09.000Z"));
    const aggregate = snapshot.windows[0];
    expect(aggregate?.windowEnd.getTime() - aggregate?.windowStart.getTime()).toBe(10_000);
    expect(snapshot.alerts[0]?.message).toContain("configured alert threshold");
  });
});
