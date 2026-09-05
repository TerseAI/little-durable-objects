import assert from "node:assert/strict"
import { test } from "node:test"

import { canonicalRegionForModal, modalPlacement } from "./regions.js"

test("canonical regions map Modal placement", () => {
    assert.deepEqual(modalPlacement("north-america-east"), {
        regions: ["us-east"],
        cloud: "gcp",
        observedPlacements: ["gcp:us-east*"]
    })
    assert.equal(canonicalRegionForModal("CLOUD_PROVIDER_GCP", "us-east4"), "north-america-east")
    assert.equal(canonicalRegionForModal("GCP", "US-EAST4-A"), "north-america-east")
})

for (const [region, pool, observedRegion] of [
    ["north-america-central", "us-central", "us-central2"],
    ["north-america-west", "us-west", "us-west2"]
] as const) {
    test(`${region} uses a broad GCP pool with public routing`, () => {
        assert.deepEqual(modalPlacement(region), {
            regions: [pool],
            cloud: "gcp",
            observedPlacements: [`gcp:${pool}*`]
        })
        assert.equal(canonicalRegionForModal("CLOUD_PROVIDER_GCP", observedRegion), region)
        assert.equal(canonicalRegionForModal("GCP", `${observedRegion.toUpperCase()}-A`), region)
        assert.equal(canonicalRegionForModal("aws", observedRegion), undefined)
    })
}

test("an unknown provider placement does not silently select a home", () => {
    assert.equal(canonicalRegionForModal("aws", "us-east-1"), undefined)
    assert.throws(() => modalPlacement("unconfigured"), /has no Modal placement/u)
})
