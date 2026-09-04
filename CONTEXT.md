# Little Durable Objects

Little Durable Objects runs stateful actors in regional sandbox hosts while a control plane owns placement, leases, and durable state coordination.

## Language

**Latency timeline**:
The ordered milestones emitted when one operation completes. Each milestone records its monotonic elapsed offset from that process-local operation's start; adjacent offsets can be subtracted during analysis without depending on synchronized clocks or implying a distributed trace.
_Avoid_: Trace, span, wall-clock timeline

**Provider boot gap**:
The Modal-owned interval between scheduled sandbox creation and the actor-host process beginning. Little Durable Objects records its surrounding boundaries and the Modal resource identifiers, while Modal's supported dashboard timeline remains the source for scheduled, started, and ready transitions.
_Avoid_: Host startup time

**Latency event family**:
One of the six completion-event schemas used for runtime diagnosis: client invocation, target resolution, host provisioning, host startup, host invocation, or state write. Related events share existing request, host, and session identifiers but do not form a distributed trace.
_Avoid_: Span, trace event
