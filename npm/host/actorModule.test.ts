import assert from "node:assert/strict"
import { mkdir, mkdtemp, realpath, rm, writeFile } from "node:fs/promises"
import os from "node:os"
import path from "node:path"
import { test } from "node:test"
import { fileURLToPath, pathToFileURL } from "node:url"

import { ActorConfigurationError, ActorDefinitionError } from "../shared/errors.js"

import { loadActorEntrypoint, resolveActorEntrypoint } from "./actorModule.js"

test("resolves the conventional TypeScript actor entrypoint", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-entrypoint-"))
    const previousDirectory = process.cwd()
    try {
        await mkdir(path.join(root, "src"))
        const entrypoint = path.join(root, "src/durable-objects.ts")
        await writeFile(entrypoint, "export {}\n")
        process.chdir(root)
        assert.equal(await realpath(fileURLToPath(await resolveActorEntrypoint(undefined))), await realpath(entrypoint))
    } finally {
        process.chdir(previousDirectory)
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects a configured actor entrypoint that does not exist", async () => {
    await assert.rejects(resolveActorEntrypoint("./missing-durable-objects.ts"), ActorConfigurationError)
})

test("prefers a compiled conventional entrypoint when available", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-compiled-"))
    const previousDirectory = process.cwd()
    try {
        await mkdir(path.join(root, "dist"))
        await writeFile(path.join(root, "dist/durable-objects.js"), "export {}\n")
        process.chdir(root)
        assert.equal(await realpath(fileURLToPath(await resolveActorEntrypoint(undefined))), await realpath(path.join(root, "dist/durable-objects.js")))
    } finally {
        process.chdir(previousDirectory)
        await rm(root, { recursive: true, force: true })
    }
})

test("rejects default and non-actor entrypoint exports", async () => {
    const root = await mkdtemp(path.join(os.tmpdir(), "durable-object-invalid-entrypoint-"))
    try {
        const defaultEntrypoint = path.join(root, "default.mjs")
        const nonActorEntrypoint = path.join(root, "non-actor.mjs")
        await writeFile(defaultEntrypoint, "export default class Counter {}\n")
        await writeFile(nonActorEntrypoint, "export class Counter {}\n")

        await assert.rejects(loadActorEntrypoint(pathToFileURL(defaultEntrypoint).href), error => {
            assert.ok(error instanceof ActorDefinitionError)
            assert.match(error.message, /named exports/)
            return true
        })
        await assert.rejects(loadActorEntrypoint(pathToFileURL(nonActorEntrypoint).href), error => {
            assert.ok(error instanceof ActorDefinitionError)
            assert.match(error.message, /directly extends Actor/)
            return true
        })
    } finally {
        await rm(root, { recursive: true, force: true })
    }
})
