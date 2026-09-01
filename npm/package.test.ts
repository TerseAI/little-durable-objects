import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"

interface PackageMetadata {
    readonly bin?: Record<string, string>
    readonly bugs?: { readonly url?: string }
    readonly engines?: { readonly node?: string }
    readonly exports?: Record<string, string | { readonly import?: string; readonly types?: string }>
    readonly files?: string[]
    readonly homepage?: string
    readonly license?: string
    readonly name?: string
    readonly publishConfig?: { readonly access?: string }
    readonly repository?: { readonly directory?: string; readonly url?: string }
    readonly scripts?: Record<string, string>
}

test("package metadata describes a public, buildable MIT package", async () => {
    const metadata = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")) as PackageMetadata
    assert.equal(metadata.name, "little-durable-objects")
    assert.equal(metadata.license, "MIT")
    assert.equal(metadata.engines?.node, ">=20")
    assert.equal(metadata.publishConfig?.access, "public")
    assert.equal(metadata.repository?.url, "git+https://github.com/TerseAI/little-durable-objects.git")
    assert.equal(metadata.repository?.directory, "npm")
    assert.equal(metadata.homepage, "https://github.com/TerseAI/little-durable-objects#readme")
    assert.equal(metadata.bugs?.url, "https://github.com/TerseAI/little-durable-objects/issues")
    assert.equal(metadata.bin?.["little-durable-objects-modal"], "./dist/providers/modalCli.js")
    assert.ok(metadata.files?.includes("LICENSE.md"))
    assert.equal(metadata.scripts?.prepack, "pnpm run clean && pnpm run build && pnpm run package:check")
})

test("every public module maps JavaScript and TypeScript declarations", async () => {
    const metadata = JSON.parse(await readFile(new URL("../package.json", import.meta.url), "utf8")) as PackageMetadata
    for (const path of [".", "./host", "./modal", "./providers", "./regions"]) {
        const entry = metadata.exports?.[path]
        assert.equal(typeof entry, "object", `${path} should have conditional exports`)
        if (typeof entry !== "object" || entry === null) continue
        assert.match(entry.import ?? "", /^\.\/dist\/.+\.js$/u)
        assert.match(entry.types ?? "", /^\.\/dist\/.+\.d\.ts$/u)
    }
})

test("repository and npm artifacts carry the little-durable MIT license", async () => {
    const expected = `MIT License

Copyright (c) 2026 Terse

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
`
    assert.equal(await readFile(new URL("../LICENSE.md", import.meta.url), "utf8"), expected)
    assert.equal(await readFile(new URL("../../LICENSE.md", import.meta.url), "utf8"), expected)
})
