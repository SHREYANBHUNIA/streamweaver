import type { RequestHandler } from "express";
import { notifyOwner } from "./_core/notification";

export type EngineAlertPayload = {
  id: string;
  kind: string;
  message: string;
  windowId?: string;
  sum?: number;
  threshold?: number;
  createdAt?: string;
};

type Notify = (payload: { title: string; content: string }) => Promise<boolean>;

function bearerToken(value: string | undefined): string {
  return value?.replace(/^Bearer\s+/i, "").trim() ?? "";
}

function isAlertPayload(value: unknown): value is EngineAlertPayload {
  if (!value || typeof value !== "object") return false;
  const alert = value as Record<string, unknown>;
  return typeof alert.id === "string" && typeof alert.kind === "string" && typeof alert.message === "string";
}

// This bridge keeps the Go worker isolated from Manus credentials. The worker
// posts a signed alert to this endpoint; the dashboard service uses the built-in
// owner channel only after authentication succeeds.
export function createOwnerAlertBridge(token: string, dispatch: Notify = notifyOwner): RequestHandler {
  return async (request, response) => {
    if (!token || bearerToken(request.header("authorization")) !== token) {
      response.status(401).json({ error: "invalid StreamWeaver alert token" });
      return;
    }
    if (!isAlertPayload(request.body)) {
      response.status(400).json({ error: "invalid StreamWeaver alert payload" });
      return;
    }

    const alert = request.body;
    const detail = [
      alert.message,
      alert.windowId ? `Window: ${alert.windowId}` : null,
      typeof alert.sum === "number" ? `SUM(amount): ${alert.sum}` : null,
      typeof alert.threshold === "number" ? `Configured alert threshold: ${alert.threshold}` : null,
      alert.createdAt ? `Occurred: ${alert.createdAt}` : null,
    ].filter(Boolean).join("\n");

    try {
      const notified = await dispatch({ title: `StreamWeaver ${alert.kind.replaceAll("_", " ")}`, content: detail });
      response.status(202).json({ accepted: true, notified });
    } catch (error) {
      console.warn("[StreamWeaver] owner alert bridge failed:", error);
      response.status(502).json({ error: "owner notification dispatch failed" });
    }
  };
}
