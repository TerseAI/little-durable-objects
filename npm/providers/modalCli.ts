#!/usr/bin/env node
import { performance } from "node:perf_hooks"
import { stdin, stdout } from "node:process"

import type { SandboxProviderCommand } from "./types.js"

async function main(): Promise<void> {
    const startedAt = performance.now()
    const chunks: Buffer[] = []
    for await (const chunk of stdin) chunks.push(Buffer.from(chunk))
    const command = JSON.parse(Buffer.concat(chunks).toString("utf8")) as SandboxProviderCommand
    const inputParsedAtMs = elapsedMs(startedAt)
    const { ModalSandboxProvider } = await import("./modal.js")
    const sdkLoadedAtMs = elapsedMs(startedAt)
    const provider = new ModalSandboxProvider()
    switch (command.operation) {
        case "ensure_host": {
            const providerStartedAtMs = elapsedMs(startedAt)
            const handle = await provider.ensureHost(command.request)
            const provisioning = handle.provisioning && {
                ...offsetProvisioning(handle.provisioning, providerStartedAtMs),
                inputParsedAtMs,
                sdkLoadedAtMs
            }
            stdout.write(JSON.stringify({ ...handle, provisioning }))
            return
        }
        case "public_host_route":
            stdout.write(JSON.stringify(await provider.publicHostRoute(command.request)))
            return
        case "warm_image":
            stdout.write(JSON.stringify(await provider.warmImage(command.request)))
            return
        case "terminate_hosts":
            stdout.write(JSON.stringify(await provider.terminateHosts(command.request)))
            return
        default:
            throw new Error("unsupported sandbox operation")
    }
}

function offsetProvisioning<T extends object>(provisioning: T, offsetMs: number): T {
    return Object.fromEntries(
        Object.entries(provisioning as Record<string, unknown>).map(([name, value]) => [name, name.endsWith("AtMs") && typeof value === "number" ? value + offsetMs : value])
    ) as T
}

function elapsedMs(startedAt: number): number {
    return Math.max(0, Math.round(performance.now() - startedAt))
}

main().catch(error => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
})
