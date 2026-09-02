import { NotFoundError } from "modal"
import type { ModalClient, SandboxCreateParams } from "modal"
import assert from "node:assert/strict"
import { test } from "node:test"

import { ModalSandboxProvider } from "./modal.js"
import type { EnsureHostRequest } from "./types.js"

test("creates a V2 Modal sandbox with the host as its main process and reports provisioning timings", async () => {
    let createOptions: SandboxCreateParams | undefined
    let usedLegacyCreate = false
    const files = new Map<string, string>()
    const sandbox = {
        sandboxId: "sb-v2-actor",
        async exec(command: string[]) {
            const placement = command.join(" ").includes("MODAL_CLOUD_PROVIDER")
            return {
                stdout: {
                    async readText() {
                        return placement ? "gcp\nus-east-1\n" : ""
                    }
                },
                stderr: {
                    async readText() {
                        return ""
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
        filesystem: {
            async writeText(contents: string, path: string) {
                files.set(path, contents)
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
            async experimentalFromName() {
                throw new NotFoundError("not found")
            },
            async experimentalCreate(_app: unknown, _image: unknown, options: SandboxCreateParams) {
                createOptions = options
                return sandbox
            },
            async create() {
                usedLegacyCreate = true
                throw new Error("legacy Sandbox creation must not be used")
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient, now: () => 0 })

    const handle = await provider.ensureHost(request())

    assert.deepEqual(handle, {
        hostId: "host.v1.project-1.00000000-0000-4000-8000-000000000001",
        route: "https://host.example.com",
        canonicalRegion: "north-america-east",
        provisioning: {
            provider: "modal",
            resourceId: "sb-v2-actor",
            reused: false,
            resourceLookupMs: 0,
            existingLookupMs: 0,
            createMs: 0,
            placementMs: 0,
            tunnelMs: 0,
            readyMs: 0,
            metadataMs: 0,
            totalMs: 0
        }
    })
    assert.equal(usedLegacyCreate, false)
    assert.equal(createOptions?.timeoutMs, 86_400_000)
    assert.equal(createOptions?.idleTimeoutMs, 300_000)
    assert.equal(createOptions?.command?.[5], "/usr/local/bin/little-durable-objects")
    assert.equal(createOptions?.command?.[8], "/tmp/durable-object-ready")
    assert.match(createOptions?.command?.[2] ?? "", /if ! test -f "\$5"; then sleep 60; fi/u)
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_TOKEN, "host-jwt")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_ACTOR_IDLE_TIMEOUT_MS, "60000")
    assert.equal(createOptions?.env?.DURABLE_OBJECT_HOST_IDLE_TIMEOUT_MS, "300000")
    assert.equal(createOptions?.env?.MODAL_TOKEN_ID, undefined)
    assert.equal(files.get("/tmp/durable-object-route"), "https://host.example.com")
    assert.match(files.get("/tmp/durable-object-host.json") ?? "", /host\.v1\.project-1/u)
})

test("surfaces host stderr when the main process exits before readiness", async () => {
    let terminated = false
    const sandbox = {
        sandboxId: "sb-v2-failed",
        async exec(command: string[]) {
            const placement = command.join(" ").includes("MODAL_CLOUD_PROVIDER")
            return {
                stdout: {
                    async readText() {
                        return placement ? "gcp\nus-east-1\n" : ""
                    }
                },
                stderr: {
                    async readText() {
                        return placement ? "" : "host could not register its lease\n"
                    }
                },
                async wait() {
                    return placement ? 0 : 1
                }
            }
        },
        async tunnels() {
            return { 7101: { url: "https://host.example.com" } }
        },
        filesystem: {
            async writeText() {}
        },
        async terminate() {
            terminated = true
        }
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
        sandboxes: {
            async experimentalFromName() {
                throw new NotFoundError("not found")
            },
            async experimentalCreate() {
                return sandbox
            }
        }
    }
    const provider = new ModalSandboxProvider({ client: client as unknown as ModalClient })

    await assert.rejects(provider.ensureHost(request()), /durable-object host did not become ready: host could not register its lease/u)
    assert.equal(terminated, true)
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
        imageRef: "im-actor",
        workingDirectory: "/workspace",
        actorEntrypoint: "src/durable-objects.ts",
        actorIdleTimeoutMs: 60_000,
        hostIdleTimeoutMs: 300_000
    }
}
