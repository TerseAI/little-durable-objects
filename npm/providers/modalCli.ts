#!/usr/bin/env node
import { stdin, stdout } from "node:process"

import { ModalSandboxProvider } from "./modal.js"
import type { SandboxProviderCommand } from "./types.js"

async function main(): Promise<void> {
    const chunks: Buffer[] = []
    for await (const chunk of stdin) chunks.push(Buffer.from(chunk))
    const command = JSON.parse(Buffer.concat(chunks).toString("utf8")) as SandboxProviderCommand
    const provider = new ModalSandboxProvider()
    if (command.operation !== "ensure_host") throw new Error(`unsupported sandbox operation ${String(command.operation)}`)
    stdout.write(JSON.stringify(await provider.ensureHost(command.request)))
}

main().catch(error => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
})
