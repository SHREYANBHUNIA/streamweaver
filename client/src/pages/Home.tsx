import { trpc } from "@/lib/trpc";
import { cn } from "@/lib/utils";
import { formatMoney, formatNumber, formatTime, queueTone, telemetrySourceLabel } from "@/lib/streamweaverPresentation";
import {
  Activity,
  BellRing,
  Boxes,
  Braces,
  Check,
  ChevronRight,
  CircleDotDashed,
  Clock3,
  Database,
  Gauge,
  Layers3,
  RefreshCw,
  Server,
  ShieldCheck,
  TriangleAlert,
  Waves,
  Zap,
} from "lucide-react";
import { toast } from "sonner";

function BlueprintMark() {
  return (
    <div className="relative grid h-9 w-9 place-items-center border border-cyan-100/80 bg-cyan-100/5 shadow-[0_0_20px_rgba(103,232,249,.12)]">
      <span className="absolute left-1/2 top-0 h-full border-l border-cyan-100/25" />
      <span className="absolute top-1/2 left-0 w-full border-t border-cyan-100/25" />
      <Waves className="relative h-4 w-4 text-cyan-100" strokeWidth={1.5} />
    </div>
  );
}

function MetricCard({ label, value, suffix, detail, tone = "cyan", icon: Icon }: { label: string; value: string; suffix?: string; detail: string; tone?: "cyan" | "amber" | "white"; icon: typeof Gauge }) {
  const tones = {
    cyan: "text-cyan-200 border-cyan-200/30 bg-cyan-200/8",
    amber: "text-amber-200 border-amber-200/30 bg-amber-200/8",
    white: "text-white border-white/20 bg-white/8",
  };
  return (
    <article className="technical-card group relative min-h-[150px] overflow-hidden p-4 sm:p-5">
      <span className="dimension-line left-4 right-4 top-2" />
      <div className="flex items-start justify-between gap-4">
        <p className="technical-label">{label}</p>
        <span className={cn("grid h-8 w-8 place-items-center border", tones[tone])}><Icon className="h-4 w-4" strokeWidth={1.5} /></span>
      </div>
      <div className="mt-7 flex items-end gap-2">
        <span className="text-3xl font-semibold tracking-[-0.04em] text-white sm:text-[2.1rem]">{value}</span>
        {suffix ? <span className="mb-1 font-mono text-xs text-cyan-100/55">{suffix}</span> : null}
      </div>
      <p className="mt-3 font-mono text-[10px] uppercase tracking-[0.13em] text-cyan-100/45">{detail}</p>
      <span className="absolute bottom-0 right-0 h-6 w-6 border-l border-t border-cyan-100/20 transition-all duration-200 group-hover:h-9 group-hover:w-9" />
    </article>
  );
}

function StatusDot({ active = true }: { active?: boolean }) {
  return <span className={cn("relative inline-flex h-2 w-2 rounded-full", active ? "bg-emerald-300" : "bg-amber-300")}><span className={cn("absolute inset-0 animate-ping rounded-full opacity-50", active ? "bg-emerald-300" : "bg-amber-300")} /></span>;
}

function Sparkline() {
  return (
    <svg viewBox="0 0 360 92" className="h-24 w-full overflow-visible" preserveAspectRatio="none" aria-label="Throughput trend">
      <defs>
        <linearGradient id="areaFill" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="#67e8f9" stopOpacity=".24" />
          <stop offset="100%" stopColor="#67e8f9" stopOpacity="0" />
        </linearGradient>
      </defs>
      {[18, 46, 74].map((y) => <line key={y} x1="0" x2="360" y1={y} y2={y} stroke="rgba(207,250,254,.16)" strokeDasharray="2 7" />)}
      <path d="M0 74 L28 63 L52 67 L75 48 L102 53 L125 30 L150 43 L175 38 L197 49 L221 18 L245 31 L271 27 L298 44 L323 21 L360 29 L360 92 L0 92 Z" fill="url(#areaFill)" />
      <path d="M0 74 L28 63 L52 67 L75 48 L102 53 L125 30 L150 43 L175 38 L197 49 L221 18 L245 31 L271 27 L298 44 L323 21 L360 29" fill="none" stroke="#a5f3fc" strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
      <circle cx="323" cy="21" r="3.5" fill="#a5f3fc" /><circle cx="323" cy="21" r="8" fill="none" stroke="#a5f3fc" strokeOpacity=".4" />
    </svg>
  );
}

export default function Home() {
  const snapshotQuery = trpc.streamweaver.snapshot.useQuery(undefined, { refetchInterval: 3_000, refetchOnWindowFocus: false });
  const snapshot = snapshotQuery.data;
  const sourceIsEngine = snapshot?.source === "engine";
  const isOnline = snapshot?.status === "running";

  const refresh = async () => {
    const result = await snapshotQuery.refetch();
    if (result.error) {
      toast.error("Telemetry refresh failed", { description: result.error.message });
      return;
    }
    toast.success("Telemetry refreshed", { description: result.data?.source === "engine" ? "Live Go engine data is connected." : "Showing clearly labelled demo telemetry." });
  };

  return (
    <div className="blueprint-shell min-h-screen overflow-hidden text-cyan-50">
      <div className="blueprint-corner left-5 top-5 hidden lg:block" />
      <div className="blueprint-corner bottom-5 right-5 hidden rotate-180 lg:block" />
      <header className="relative z-10 flex min-h-20 items-center justify-between border-b border-cyan-100/20 px-5 sm:px-8 lg:px-10">
        <div className="flex items-center gap-3">
          <BlueprintMark />
          <div>
            <p className="text-lg font-bold leading-none tracking-[-0.045em] text-white">StreamWeaver</p>
            <p className="mt-1 font-mono text-[9px] uppercase tracking-[0.2em] text-cyan-100/55">Incremental stream observatory</p>
          </div>
        </div>
        <div className="hidden items-center gap-5 md:flex">
          <div className="border-r border-cyan-100/20 pr-5 text-right">
            <p className="technical-label">Pipeline</p>
            <p className="mt-1 font-mono text-xs text-cyan-50">{snapshot?.pipelineId ?? "CONNECTING"}</p>
          </div>
          <div className="flex items-center gap-2 border border-cyan-100/25 bg-cyan-50/5 px-3 py-2">
            <StatusDot active={isOnline} />
            <span className="font-mono text-[10px] font-semibold uppercase tracking-[0.15em] text-cyan-50">{isOnline ? "Operational" : "Awaiting engine"}</span>
          </div>
          <button onClick={refresh} className="technical-icon-button" aria-label="Refresh telemetry"><RefreshCw className={cn("h-4 w-4", snapshotQuery.isFetching && "animate-spin")} /></button>
        </div>
      </header>

      <div className="relative z-10 grid min-h-[calc(100vh-80px)] lg:grid-cols-[222px_minmax(0,1fr)]">
        <aside className="hidden border-r border-cyan-100/20 bg-[#05132c]/55 p-5 lg:block">
          <p className="technical-label px-2">Command plane</p>
          <nav className="mt-5 space-y-1" aria-label="Dashboard navigation">
            {[{ label: "Overview", icon: Gauge, active: true }, { label: "Pipelines", icon: Layers3 }, { label: "Windows", icon: CircleDotDashed }, { label: "State store", icon: Database }, { label: "Checkpoints", icon: ShieldCheck }, { label: "Alerts", icon: BellRing }].map(({ label, icon: Icon, active }) => (
              <button key={label} onClick={() => !active && toast.message("Section overview is in development", { description: "This single-page observatory currently keeps its detailed panels in Overview." })} className={cn("group flex w-full items-center gap-3 border px-3 py-3 text-left transition-colors", active ? "border-cyan-100/45 bg-cyan-100/10 text-white" : "border-transparent text-cyan-100/55 hover:border-cyan-100/20 hover:bg-cyan-100/5 hover:text-cyan-50")}>
                <Icon className="h-4 w-4" strokeWidth={1.4} />
                <span className="font-mono text-[11px] uppercase tracking-[0.11em]">{label}</span>
                {active ? <ChevronRight className="ml-auto h-3.5 w-3.5" /> : null}
              </button>
            ))}
          </nav>
          <div className="mt-10 technical-frame p-4">
            <p className="technical-label">Recovery guarantee</p>
            <p className="mt-3 text-sm leading-5 text-cyan-50/85">Durable state, atomic checkpoint, then offset commit.</p>
            <div className="mt-4 flex items-center gap-2 font-mono text-[10px] uppercase tracking-[0.13em] text-emerald-200"><Check className="h-3.5 w-3.5" /> Defined behavior</div>
          </div>
        </aside>

        <main className="min-w-0 p-5 sm:p-8 lg:p-9">
          <div className="mx-auto max-w-[1540px]">
            <section className="flex flex-col justify-between gap-6 xl:flex-row xl:items-end">
              <div>
                <div className="flex items-center gap-3"><span className="dimension-chip">01 / LIVE SYSTEM</span><span className="h-px w-14 bg-cyan-100/35" /></div>
                <h1 className="mt-4 text-3xl font-bold tracking-[-0.055em] text-white sm:text-4xl">Transaction processing,<br /><span className="text-cyan-200">drawn to tolerance.</span></h1>
                <p className="mt-4 max-w-2xl text-sm leading-6 text-cyan-50/62">Operational view of the Go transaction engine: Kafka ingress, event-time windows, RocksDB state, checkpoint recovery, and alerting without replaying the full dataset.</p>
              </div>
              <div className="flex items-center gap-3 self-start xl:self-auto">
                <span className={cn("border px-3 py-2 font-mono text-[10px] uppercase tracking-[0.14em]", sourceIsEngine ? "border-emerald-200/40 bg-emerald-200/10 text-emerald-100" : "border-amber-200/40 bg-amber-200/10 text-amber-100")}>{telemetrySourceLabel(snapshot?.source ?? "demo")}</span>
                <button onClick={refresh} className="technical-button"><RefreshCw className={cn("h-3.5 w-3.5", snapshotQuery.isFetching && "animate-spin")} /> Sync</button>
              </div>
            </section>

            {snapshot?.lastError ? <div className="mt-6 flex gap-3 border border-amber-200/40 bg-amber-100/10 p-4 text-amber-50"><TriangleAlert className="mt-0.5 h-4 w-4 shrink-0" /><p className="text-sm leading-5"><strong>Go API unavailable.</strong> The interface has switched to clearly marked demo telemetry. <span className="font-mono text-xs opacity-80">{snapshot.lastError}</span></p></div> : null}

            <section className="mt-8 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              <MetricCard label="Event throughput" value={snapshot ? formatNumber(snapshot.throughputPerSecond) : "—"} suffix="EVT/S" detail="Kafka to operator ingress" icon={Activity} />
              <MetricCard label="P95 processing latency" value={snapshot ? String(snapshot.p95LatencyMs) : "—"} suffix="MS" detail="Event in to state sync" icon={Clock3} tone="white" />
              <MetricCard label="Backpressure" value={snapshot ? `${snapshot.queueDepth}/${snapshot.queueCapacity}` : "—"} detail="Bounded queue occupancy" icon={Gauge} tone={snapshot ? queueTone(snapshot.queueDepth, snapshot.queueCapacity) : "cyan"} />
              <MetricCard label="Checkpoint sequence" value={snapshot ? formatNumber(snapshot.metrics.checkpointCount) : "—"} suffix="SAVED" detail="Atomic latest manifest" icon={ShieldCheck} />
            </section>

            <section className="mt-4 grid gap-4 2xl:grid-cols-[minmax(0,1.55fr)_minmax(320px,.8fr)]">
              <article className="technical-card overflow-hidden p-5 sm:p-6">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div><p className="technical-label">Pipeline diagram / transaction-demo</p><h2 className="mt-2 text-xl font-semibold tracking-[-0.04em] text-white">Event-time aggregation path</h2></div>
                  <div className="font-mono text-[10px] uppercase tracking-[0.12em] text-cyan-100/55">Watermark {snapshot ? formatTime(snapshot.watermark) : "—"} UTC</div>
                </div>
                <div className="blueprint-flow mt-7 grid gap-3 md:grid-cols-5">
                  {[{ title: "Kafka", note: "transactions", icon: Server }, { title: "Filter + map", note: "incremental", icon: Braces }, { title: "Window", note: "10 seconds", icon: Clock3 }, { title: "SUM(amount)", note: "RocksDB state", icon: Database }, { title: "Alert", note: "threshold", icon: BellRing }].map(({ title, note, icon: Icon }, index) => <div className="flow-node relative" key={title}><div className="flex items-center justify-between"><span className="font-mono text-[10px] text-cyan-100/45">0{index + 1}</span><Icon className="h-4 w-4 text-cyan-100" strokeWidth={1.25} /></div><p className="mt-6 text-sm font-semibold text-white">{title}</p><p className="mt-1 font-mono text-[10px] uppercase tracking-[0.1em] text-cyan-100/55">{note}</p></div>)}
                </div>
                <div className="mt-7 grid gap-5 border-t border-cyan-100/20 pt-5 lg:grid-cols-[.85fr_1.15fr]">
                  <div><p className="technical-label">Ingress momentum</p><p className="mt-2 font-mono text-xs text-cyan-100/55">± 15 second throughput profile</p></div>
                  <Sparkline />
                </div>
              </article>

              <article className="technical-card relative overflow-hidden p-5 sm:p-6">
                <div className="flex items-start justify-between"><div><p className="technical-label">State + recovery</p><h2 className="mt-2 text-xl font-semibold tracking-[-0.04em] text-white">Checkpoint contract</h2></div><Database className="h-5 w-5 text-cyan-100" strokeWidth={1.25} /></div>
                <div className="mt-6 space-y-4">
                  {[{ label: "State backend", value: "RocksDB / durable", icon: Database }, { label: "Checkpoint", value: snapshot ? formatTime(snapshot.checkpoint.createdAt) + " UTC" : "—", icon: ShieldCheck }, { label: "Source offsets", value: snapshot ? `${snapshot.checkpoint.offsetCount} partitions` : "—", icon: Server }].map(({ label, value, icon: Icon }) => <div className="flex items-center gap-3 border-l border-cyan-100/30 pl-3" key={label}><Icon className="h-4 w-4 text-cyan-100/75" strokeWidth={1.3} /><div><p className="technical-label">{label}</p><p className="mt-1 text-sm text-white">{value}</p></div></div>)}
                </div>
                <p className="mt-6 border-t border-cyan-100/15 pt-4 text-xs leading-5 text-cyan-50/60">{snapshot?.recoveryPolicy ?? "Loading recovery contract…"}</p>
              </article>
            </section>

            <section className="mt-4 grid gap-4 xl:grid-cols-[minmax(0,1.5fr)_minmax(300px,.75fr)]">
              <article className="technical-card overflow-hidden">
                <div className="flex items-center justify-between border-b border-cyan-100/20 px-5 py-4 sm:px-6"><div><p className="technical-label">Event-time window register</p><h2 className="mt-1 text-lg font-semibold tracking-[-0.03em] text-white">SUM(amount) / 10-second windows</h2></div><span className="font-mono text-[10px] uppercase tracking-[0.12em] text-cyan-100/55">{snapshot?.windows.length ?? 0} tracked</span></div>
                <div className="overflow-x-auto"><table className="w-full min-w-[620px] text-left"><thead><tr className="technical-label border-b border-cyan-100/10"><th className="px-5 py-3 font-medium sm:px-6">Window boundary</th><th className="px-4 py-3 font-medium">Partition key</th><th className="px-4 py-3 text-right font-medium">SUM(amount)</th><th className="px-4 py-3 text-right font-medium">Events</th><th className="px-5 py-3 text-right font-medium sm:px-6">State</th></tr></thead><tbody>{snapshot?.windows.map((window) => <tr key={window.id} className="border-b border-cyan-100/10 last:border-0"><td className="px-5 py-4 font-mono text-xs text-cyan-50/80 sm:px-6">{formatTime(window.windowStart)}—{formatTime(window.windowEnd)}</td><td className="px-4 py-4 text-sm text-white">{window.key}</td><td className="px-4 py-4 text-right font-mono text-sm text-cyan-100">{formatMoney(window.sum)}</td><td className="px-4 py-4 text-right font-mono text-sm text-cyan-50/70">{window.count}</td><td className="px-5 py-4 text-right sm:px-6"><span className={cn("inline-flex border px-2 py-1 font-mono text-[9px] uppercase tracking-[0.12em]", window.emitted ? "border-emerald-200/30 bg-emerald-200/10 text-emerald-100" : "border-cyan-100/30 bg-cyan-100/10 text-cyan-50")}>{window.emitted ? "Emitted" : "Open"}</span></td></tr>)}</tbody></table></div>
              </article>
              <aside className="technical-card p-5 sm:p-6"><div className="flex items-start justify-between"><div><p className="technical-label">Alert bus</p><h2 className="mt-2 text-xl font-semibold tracking-[-0.04em] text-white">Active signals</h2></div><BellRing className="h-5 w-5 text-amber-200" strokeWidth={1.3} /></div><div className="mt-6 space-y-4">{snapshot?.alerts.length ? snapshot.alerts.map((alert) => <div className="border border-amber-200/30 bg-amber-100/8 p-4" key={alert.id}><div className="flex items-center gap-2"><TriangleAlert className="h-4 w-4 text-amber-200" /><p className="font-mono text-[10px] uppercase tracking-[0.13em] text-amber-100">{alert.kind.replaceAll("_", " ")}</p></div><p className="mt-3 text-sm leading-5 text-white">{alert.message}</p><div className="mt-4 flex items-center justify-between border-t border-amber-100/20 pt-3 font-mono text-[10px] text-amber-100/70"><span>{alert.sum ? formatMoney(alert.sum) : "Recovery"}</span><span>{formatTime(alert.createdAt)}</span></div></div>) : <p className="text-sm text-cyan-50/60">No active alerts.</p>}</div><div className="mt-5 flex items-center gap-2 border-t border-cyan-100/15 pt-4"><Zap className="h-4 w-4 text-cyan-100" /><p className="font-mono text-[10px] uppercase tracking-[0.12em] text-cyan-100/60">Owner webhook dispatch enabled</p></div></aside>
            </section>

            <section className="mt-4 grid gap-4 md:grid-cols-3"><div className="technical-frame p-4"><p className="technical-label">Late events</p><p className="mt-3 text-2xl font-semibold tracking-tight text-white">{snapshot ? formatNumber(snapshot.metrics.lateSideOutput + snapshot.metrics.lateDropped + snapshot.metrics.lateAccumulated) : "—"}</p><p className="mt-1 font-mono text-[10px] uppercase tracking-[0.11em] text-cyan-100/50">Watermark policy applied</p></div><div className="technical-frame p-4"><p className="technical-label">Duplicate suppression</p><p className="mt-3 text-2xl font-semibold tracking-tight text-white">{snapshot ? formatNumber(snapshot.metrics.duplicates) : "—"}</p><p className="mt-1 font-mono text-[10px] uppercase tracking-[0.11em] text-cyan-100/50">Persisted event ID ledger</p></div><div className="technical-frame p-4"><p className="technical-label">Recovery failures</p><p className="mt-3 text-2xl font-semibold tracking-tight text-white">{snapshot ? formatNumber(snapshot.metrics.recoveryFailures) : "—"}</p><p className="mt-1 font-mono text-[10px] uppercase tracking-[0.11em] text-cyan-100/50">Owner-notified exception path</p></div></section>
          </div>
        </main>
      </div>
      <footer className="relative z-10 flex flex-wrap items-center justify-between gap-3 border-t border-cyan-100/20 px-5 py-3 font-mono text-[9px] uppercase tracking-[0.12em] text-cyan-100/45 sm:px-8"><span>SW / TXN / 10S / REV 1.0</span><span className="flex items-center gap-2"><Boxes className="h-3.5 w-3.5" /> Go engine · Kafka · RocksDB</span></footer>
    </div>
  );
}
