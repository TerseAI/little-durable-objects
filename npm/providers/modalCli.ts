#!/usr/bin/env node
import { stdin, stdout } from "node:process"

import { ModalSandboxProvider } from "./modal.js"
import type { SandboxProviderCommand } from "./types.js"

async function main(): Promise<void> {
    const chunks: Buffer[] = []
    for await (const chunk of stdin) chunks.push(Buffer.from(chunk))
    const command = JSON.parse(Buffer.concat(chunks).toString("utf8")) as SandboxProviderCommand
    const provider = new ModalSandboxProvider()
    switch (command.operation) {
        case "ensure_host":
            stdout.write(JSON.stringify(await provider.ensureHost(command.request)))
            return
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

main().catch(error => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
})
