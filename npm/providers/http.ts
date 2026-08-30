import { timingSafeEqual } from "node:crypto"
import { createServer } from "node:http"
import type { IncomingMessage, Server, ServerResponse } from "node:http"
import { z } from "zod"

import type { EnsureHostRequest, SandboxProvider } from "./types.js"

const requestSchema = z.object({
    namespaceId: z.string().regex(/^[A-Za-z0-9._-]{1,96}$/u),
    principalId: z.string().min(1).max(255),
    credentialId: z.string().min(1).max(255),
    codeRevision: z.string().min(1).max(128),
    canonicalRegion: z.string().regex(/^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$/u)
})

interface SandboxProviderServerOptions {
    readonly provider: SandboxProvider
    readonly token: string
    readonly host?: string
    readonly port?: number
}

async function startSandboxProviderServer(options: SandboxProviderServerOptions): Promise<Server> {
    if (!options.token || options.token.trim() !== options.token) throw new Error("sandbox-provider token must be non-empty without surrounding whitespace")
    const server = createServer((request, response) => void handle(options, request, response))
    await new Promise<void>((resolve, reject) => {
        server.once("error", reject)
        server.listen(options.port ?? 7200, options.host ?? "127.0.0.1", () => {
            server.off("error", reject)
            resolve()
        })
    })
    return server
}

async function handle(options: SandboxProviderServerOptions, request: IncomingMessage, response: ServerResponse): Promise<void> {
    try {
        if (!authorized(request.headers.authorization, options.token)) return json(response, 401, { error: "unauthorized" })
        if (request.method !== "POST") return json(response, 405, { error: "method not allowed" })
        const parsed = requestSchema.safeParse(await readJson(request))
        if (!parsed.success) return json(response, 400, { error: "invalid host request" })
        const hostRequest: EnsureHostRequest = parsed.data
        switch (request.url) {
            case "/hosts/ensure":
                return json(response, 200, await options.provider.ensureHost(hostRequest))
            case "/hosts/status":
                return json(response, 200, { status: await options.provider.status(hostRequest) })
            case "/hosts/deactivate":
                await options.provider.deactivate(hostRequest)
                return json(response, 204)
            case "/hosts/remove-local-cache":
                await options.provider.removeLocalCache(hostRequest)
                return json(response, 204)
            default:
                return json(response, 404, { error: "not found" })
        }
    } catch (error) {
        return json(response, 503, { error: error instanceof Error ? error.message : String(error) })
    }
}

function authorized(header: string | undefined, expected: string): boolean {
    const supplied = header?.startsWith("Bearer ") ? header.slice(7) : ""
    const suppliedBytes = Buffer.from(supplied)
    const expectedBytes = Buffer.from(expected)
    return suppliedBytes.length === expectedBytes.length && timingSafeEqual(suppliedBytes, expectedBytes)
}

async function readJson(request: IncomingMessage): Promise<unknown> {
    const chunks: Buffer[] = []
    let bytes = 0
    for await (const chunk of request) {
        const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk)
        bytes += buffer.length
        if (bytes > 64 * 1024) throw new Error("sandbox-provider request is too large")
        chunks.push(buffer)
    }
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown
}

function json(response: ServerResponse, status: number, body?: unknown): void {
    response.statusCode = status
    if (body === undefined) {
        response.end()
        return
    }
    response.setHeader("content-type", "application/json")
    response.end(JSON.stringify(body))
}

export { startSandboxProviderServer }
export type { SandboxProviderServerOptions }
