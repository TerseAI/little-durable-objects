import { once } from "node:events"
import { performance } from "node:perf_hooks"
import type { Readable, Writable } from "node:stream"

import type { SandboxProvider, SandboxProviderCommand } from "./types.js"

const MAX_COMMAND_BYTES = 1024 * 1024

async function runProviderCommands(input: Readable, output: Writable, createProvider: () => Promise<SandboxProvider>, persistent: boolean): Promise<void> {
    let provider: SandboxProvider | undefined
    for await (const document of commandDocuments(input, persistent)) {
        const startedAt = performance.now()
        let response: unknown
        try {
            const command = JSON.parse(document) as SandboxProviderCommand
            const inputParsedAtMs = elapsedMs(startedAt)
            provider ??= await createProvider()
            const sdkLoadedAtMs = elapsedMs(startedAt)
            const result = await execute(provider, command, startedAt, inputParsedAtMs, sdkLoadedAtMs)
            response = persistent ? { status: "success", result } : result
        } catch (error) {
            if (!persistent) throw error
            response = { status: "failure", error: error instanceof Error ? error.message : String(error) }
        }
        const encoded = JSON.stringify(response)
        if (Buffer.byteLength(encoded) >= MAX_COMMAND_BYTES) throw new Error("provider response is too large")
        if (!output.write(`${encoded}\n`)) await once(output, "drain")
    }
}

async function* commandDocuments(input: Readable, persistent: boolean): AsyncGenerator<string> {
    input.setEncoding("utf8")
    let buffer = ""
    for await (const chunk of input) {
        buffer += String(chunk)
        if (persistent) {
            let newline: number
            while ((newline = buffer.indexOf("\n")) !== -1) {
                const document = buffer.slice(0, newline)
                checkSize(document)
                buffer = buffer.slice(newline + 1)
                yield document
            }
        }
        checkSize(buffer)
    }
    if (buffer) {
        if (persistent) throw new Error("incomplete provider command")
        yield buffer
    }
}

function checkSize(document: string): void {
    if (Buffer.byteLength(document) > MAX_COMMAND_BYTES) throw new Error(`provider command exceeds ${MAX_COMMAND_BYTES} bytes`)
}

async function execute(provider: SandboxProvider, command: SandboxProviderCommand, startedAt: number, inputParsedAtMs: number, sdkLoadedAtMs: number): Promise<unknown> {
    switch (command.operation) {
        case "ensure_host": {
            const offsetMs = elapsedMs(startedAt)
            const handle = await provider.ensureHost(command.request)
            const provisioning = handle.provisioning && {
                ...Object.fromEntries(Object.entries(handle.provisioning).map(([name, value]) => [name, name.endsWith("AtMs") && typeof value === "number" ? value + offsetMs : value])),
                inputParsedAtMs,
                sdkLoadedAtMs
            }
            return { ...handle, provisioning }
        }
        case "public_host_route":
            return provider.publicHostRoute(command.request)
        case "warm_image":
            return provider.warmImage(command.request)
        case "terminate_hosts":
            return provider.terminateHosts(command.request)
        default:
            throw new Error("unsupported sandbox operation")
    }
}

function elapsedMs(startedAt: number): number {
    return Math.max(0, Math.round(performance.now() - startedAt))
}

export { runProviderCommands }
