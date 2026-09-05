import assert from "node:assert/strict"
import { execFileSync } from "node:child_process"
import { test } from "node:test"

import * as api from "./index.js"

test("importing actor definitions does not load remote transports or TypeScript tooling", () => {
    const entrypoint = new URL("./index.js", import.meta.url).href
    execFileSync(process.execPath, [
        "--input-type=module",
        "--eval",
        `
        import { register } from "node:module";
        register("data:text/javascript," + encodeURIComponent(\`export function resolve(specifier, context, next) {
            if (/^(?:@grpc\\\\/|protobufjs$|ws$|tsx\\\\/)/.test(specifier)) throw new Error("eager dependency: " + specifier);
            return next(specifier, context);
        }\`));
        await import(${JSON.stringify(entrypoint)});
    `
    ])
})

test("the package root exposes the complete minimal actor API", () => {
    assert.deepEqual(Object.keys(api).sort(), ["Actor", "ActorInvocationError", "configureDurableObjects"])
})
