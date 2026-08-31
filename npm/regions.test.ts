import assert from "node:assert/strict"
import { test } from "node:test"

import { canonicalRegionForModal, modalPlacement } from "./regions.js"

test("canonical regions map Modal placement", () => {
    assert.deepEqual(modalPlacement("north-america-east"), {
        regions: ["us-east"],
        cloud: "gcp",
        observedPlacements: ["gcp:us-east*"]
    })
    assert.equal(canonicalRegionForModal("gcp", "us-east-1"), "north-america-east")
    assert.equal(canonicalRegionForModal("GCP", "US-EAST4-A"), "north-america-east")
})

test("an unknown provider placement does not silently select a home", () => {
    assert.equal(canonicalRegionForModal("aws", "us-east-1"), undefined)
    assert.throws(() => modalPlacement("unconfigured"), /has no Modal placement/u)
})
