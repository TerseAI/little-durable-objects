#!/usr/bin/env node
import { stdin, stdout } from "node:process"

import { ModalSandboxProvider } from "./modal.js"
import type { ModalSandboxCommand } from "./types.js"

async function main(): Promise<void> {
    const chunks: Buffer[] = []
    for await (const chunk of stdin) chunks.push(Buffer.from(chunk))
    const command = JSON.parse(Buffer.concat(chunks).toString("utf8")) as ModalSandboxCommand
    const provider = new ModalSandboxProvider()
    switch (command.operation) {
        case "ensure_host":
            stdout.write(JSON.stringify(await provider.ensureHost(command.request)))
            return
        case "status":
            stdout.write(JSON.stringify({ status: await provider.status(command.request) }))
            return
        case "deactivate":
            await provider.deactivate(command.request)
            stdout.write("{}")
            return
        case "remove_local_cache":
            await provider.removeLocalCache(command.request)
            stdout.write("{}")
            return
        default:
            throw command satisfies never
    }
}

main().catch(error => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
})
