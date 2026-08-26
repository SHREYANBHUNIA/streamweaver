import { describe, expect, it, vi } from "vitest";
import { createOwnerAlertBridge } from "./streamweaver-alerts";

function mockResponse() {
  const response = { status: vi.fn(), json: vi.fn() };
  response.status.mockReturnValue(response);
  return response;
}

describe("StreamWeaver owner alert bridge", () => {
  it("rejects an alert without the expected bearer token", async () => {
    const dispatch = vi.fn();
    const bridge = createOwnerAlertBridge("local-token", dispatch);
    const response = mockResponse();
    await bridge({ header: () => "Bearer incorrect", body: {} } as any, response as any, vi.fn());
    expect(response.status).toHaveBeenCalledWith(401);
    expect(dispatch).not.toHaveBeenCalled();
  });

  it("relays an authenticated threshold alert through the owner channel", async () => {
    const dispatch = vi.fn().mockResolvedValue(true);
    const bridge = createOwnerAlertBridge("local-token", dispatch);
    const response = mockResponse();
    await bridge({ header: () => "Bearer local-token", body: { id: "threshold-1", kind: "threshold_crossed", message: "SUM(amount) exceeded the configured alert threshold in a 10-second window.", sum: 12_480, threshold: 10_000 } } as any, response as any, vi.fn());
    expect(dispatch).toHaveBeenCalledWith(expect.objectContaining({ title: "StreamWeaver threshold crossed", content: expect.stringContaining("SUM(amount): 12480") }));
    expect(response.status).toHaveBeenCalledWith(202);
  });
});
