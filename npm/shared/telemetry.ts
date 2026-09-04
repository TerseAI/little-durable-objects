import { performance } from "node:perf_hooks"

type TelemetryEvent = Record<string, boolean | number | string | undefined>
type TelemetrySink = (event: TelemetryEvent) => void

class LatencyTimeline {
    private readonly origin: number
    private readonly milestones: Record<string, number> = { started_at_ms: 0 }

    constructor(private readonly now: () => number = () => performance.now()) {
        this.origin = this.now()
    }

    mark(name: string): void {
        this.milestones[`${name}_at_ms`] = Math.max(0, Math.round(this.now() - this.origin))
    }

    finish(): Record<string, number> {
        this.mark("completed")
        return { ...this.milestones }
    }
}

const stderrTelemetry: TelemetrySink = event => process.stderr.write(`${JSON.stringify(event)}\n`)

export { LatencyTimeline, stderrTelemetry }
export type { TelemetryEvent, TelemetrySink }
