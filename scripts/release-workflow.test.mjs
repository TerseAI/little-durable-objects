import assert from "node:assert/strict"
import { readFileSync } from "node:fs"
import test from "node:test"

const root = new URL("../", import.meta.url)
const read = path => readFileSync(new URL(path, root), "utf8")

test("release images use the established Terse Artifact Registry", () => {
    const workflow = read(".github/workflows/release.yml")

    assert.match(workflow, /REGISTRY: us-central1-docker\.pkg\.dev/)
    assert.match(workflow, /IMAGE: us-central1-docker\.pkg\.dev\/fluid-analogy-473415-c2\/public\/little-durable-objects/)
    assert.match(workflow, /google-github-actions\/auth@/)
    assert.doesNotMatch(workflow, /ghcr\.io/)
})

test("user-facing image references match the release registry", () => {
    for (const path of ["README.md", "docs/releasing.md", "docs/system-architecture.md"]) {
        assert.doesNotMatch(read(path), /GHCR|ghcr\.io/, path)
    }
})
