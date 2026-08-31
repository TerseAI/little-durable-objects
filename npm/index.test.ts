import assert from "node:assert/strict"
import { test } from "node:test"

import * as api from "./index.js"

test("the package root exposes the complete minimal actor API", () => {
    assert.deepEqual(Object.keys(api).sort(), ["Actor", "ActorInvocationError", "configureDurableObjects"])
})
