import { NotFoundError } from "modal"
import type { ModalClient, SandboxCreateParams } from "modal"
import assert from "node:assert/strict"
import { test } from "node:test"

import { ModalSandboxProvider } from "./modal.js"
import type { EnsureHostRequest } from "./types.js"

test("creates Modal with the host as its main process and injects only runtime credentials", async () => {
    let createOptions: SandboxCreateParams | undefined
    const files = new Map<string, string>()
    const sandbox = {
        async exec(command: string[]) {
            const placement = command.join(" ").includes("MODAL_CLOUD_PROVIDER")
            return {
                stdout: {
                    async readText() {
                        return placement ? "gcp\nus-east-1\n" : ""
                    }
                },
                async wait() {
                    return 0
                }
            }
        },
        async tunnels() {
            return { 7101: { url: "https://host.example.com" } }
        },
        async open(path: string) {
            let contents = ""
            return {
                async write(value: Uint8Array) {
                    contents += new TextDecoder().decode(value)
                },
                async flush() {
                    files.set(path, contents)
                },
                async close() {}
            }
        },
        async terminate() {}
    }
    const client = {
        apps: {
            async fromName() {
                return { name: "durable-object-hosts" }
            }
        },
        images: {
            async fromId() {
                return { imageId: "im-actor" }
            }
        },
        volumes: {
            async fromName(_name: string, options?: { createIfMissing?: boolean }) {
                if (!options?.createIfMissing) throw new NotFoundError("not found")
                return { volumeId: "vo-cache" }
            },
            async delete() {}
        },
        sandboxes: {
            async fromName() {
                throw new NotFoundError("not found")
            },
            async create(_app: unknown, _image: unknown, options: SandboxCreateParams) {
                createOptions = options
                return sandbox
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient })

    const handle = await provider.ensureHost(request())

    assert.deepEqual(handle, {
        hostId: "host.v1.project-1.00000000-0000-4000-8000-000000000001",
        route: "https://host.example.com",
        canonicalRegion: "north-america-east",
        cacheSource: "durable_storage"
    })
    assert.equal(createOptions?.timeoutMs, 86_400_000)
    assert.equal(createOptions?.idleTimeoutMs, 300_000)
    assert.deepEqual(createOptions?.command?.slice(-2), ["/tmp/durable-object-route", "/usr/local/bin/durable-object-runtime"])
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_TOKEN, "host-jwt")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS, "60000")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS, "300000")
    assert.equal(createOptions?.env?.MODAL_TOKEN_ID, undefined)
    assert.equal(files.get("/tmp/durable-object-route"), "https://host.example.com")
    assert.match(files.get("/tmp/durable-object-host.json") ?? "", /host\.v1\.project-1/u)
})

function request(): EnsureHostRequest {
    return {
        namespaceId: "project-1",
        codeRevision: "revision-1",
        canonicalRegion: "north-america-east",
        hostId: "host.v1.project-1.00000000-0000-4000-8000-000000000001",
        sessionId: "00000000-0000-4000-8000-000000000002",
        hostToken: "host-jwt",
        jwtPublicKeys: '{"primary":"public-key"}',
        controlPlaneUrl: "https://objects.example.com",
        jwtIssuer: "durable-object-control-plane",
        invocationJwtAudience: "durable-object-invoke",
        modalImageId: "im-actor",
        workingDirectory: "/workspace",
        actorEntrypoint: "src/durable-objects.ts",
        actorIdleTimeoutMs: 60_000,
        hostIdleTimeoutMs: 300_000
    }
}
