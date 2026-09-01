import assert from "node:assert/strict"
import { readFile } from "node:fs/promises"
import { test } from "node:test"

import { parseVersion, readReleaseVersion, stampReleaseVersion, verifyReleaseVersion } from "./release.mjs"

const manifests = {
    cargoLock: `[[package]]
name = "little-durable-objects"
version = "0.4.7"
dependencies = []
`,
    cargoToml: `[package]
name = "little-durable-objects"
version = "0.4.7"
edition = "2024"
`,
    npmPackage: `{
    "name": "little-durable-objects",
    "version": "0.4.7"
}
`
}

test("reads one release version from every publishable manifest", () => {
    assert.equal(readReleaseVersion(manifests), "0.4.7")
    verifyReleaseVersion(manifests, "0.4.7")
})

test("rejects release manifests that have drifted", () => {
    const drifted = { ...manifests, cargoToml: manifests.cargoToml.replace("0.4.7", "0.4.6") }
    assert.throws(() => readReleaseVersion(drifted), /release manifests disagree/iu)
    assert.throws(() => verifyReleaseVersion(manifests, "0.4.8"), /do not match 0\.4\.8/iu)
})

test("stamps all release manifests without reformatting them", () => {
    const stamped = stampReleaseVersion(manifests, "0.5.0")
    assert.equal(readReleaseVersion(stamped), "0.5.0")
    assert.equal(stamped.cargoToml, manifests.cargoToml.replace("0.4.7", "0.5.0"))
    assert.equal(stamped.cargoLock, manifests.cargoLock.replace("0.4.7", "0.5.0"))
    assert.equal(stamped.npmPackage, manifests.npmPackage.replace("0.4.7", "0.5.0"))
})

test("accepts stable semantic versions only", () => {
    assert.equal(parseVersion("1.2.3"), "1.2.3")
    for (const invalid of ["v1.2.3", "1.2", "1.2.3-beta.1", "01.2.3"]) {
        assert.throws(() => parseVersion(invalid), /version must look like 1\.2\.3/iu)
    }
})

test("container builds include compile-time migration assets", async () => {
    const dockerfile = await readFile(new URL("../Dockerfile", import.meta.url), "utf8")
    assert.match(dockerfile, /^COPY migrations \.\/migrations$/mu)
})
