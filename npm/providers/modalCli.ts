#!/usr/bin/env node
import { stdin, stdout } from "node:process"

import { runProviderCommands } from "./commandProcess.js"

runProviderCommands(
    stdin,
    stdout,
    async () => {
        const { ModalSandboxProvider } = await import("./modal.js")
        return new ModalSandboxProvider()
    },
    process.argv.includes("--serve")
).catch(error => {
    process.stderr.write(`${error instanceof Error ? error.message : String(error)}\n`)
    process.exitCode = 1
})
