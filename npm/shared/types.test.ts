import assert from "node:assert/strict"
import { test } from "node:test"

import { ActorProtocolError } from "./errors.js"
import { parseActorSessionServerMessage, parseSocketEffects } from "./types.js"

test("rejects malformed actor session messages", () => {
    assert.throws(() => parseActorSessionServerMessage('{"type":"command","message_id":1,"command":{"type":"invoke","request_id":false}}'), ActorProtocolError)
})

test("rejects malformed socket effects", () => {
    assert.throws(() => parseSocketEffects([{ type: "close", connection_id: "socket-1", code: 1001, reason: "" }]), ActorProtocolError)
    assert.throws(() => parseSocketEffects({ type: "set_tags", connection_id: "socket-1", tags: [] }), ActorProtocolError)
})
