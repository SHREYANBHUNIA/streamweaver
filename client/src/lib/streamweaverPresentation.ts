export function formatNumber(value: number) {
  return new Intl.NumberFormat("en-US").format(value);
}

export function formatMoney(value: number) {
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(value);
}

export function formatTime(value: Date) {
  return new Intl.DateTimeFormat("en-US", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false, timeZone: "UTC" }).format(value);
}

export function queueTone(depth: number, capacity: number): "cyan" | "amber" {
  return capacity > 0 && depth / capacity > 0.75 ? "amber" : "cyan";
}

export function telemetrySourceLabel(source: "engine" | "demo") {
  return source === "engine" ? "Live engine feed" : "Demo telemetry";
}
