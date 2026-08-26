import { describe, expect, it } from "vitest";
import { formatMoney, formatNumber, formatTime, queueTone, telemetrySourceLabel } from "./streamweaverPresentation";

describe("StreamWeaver presentation helpers", () => {
  it("formats telemetry values for the blueprint dashboard", () => {
    expect(formatNumber(12_480)).toBe("12,480");
    expect(formatMoney(12_480)).toBe("$12,480");
    expect(formatTime(new Date("2026-08-26T12:00:05Z"))).toBe("12:00:05");
  });

  it("communicates queue pressure and telemetry provenance consistently", () => {
    expect(queueTone(768, 1024)).toBe("cyan");
    expect(queueTone(769, 1024)).toBe("amber");
    expect(telemetrySourceLabel("engine")).toBe("Live engine feed");
    expect(telemetrySourceLabel("demo")).toBe("Demo telemetry");
  });
});
