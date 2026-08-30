import { readFile, writeFile } from "node:fs/promises"
import protobuf from "protobufjs"

const protoUrl = new URL("../../proto/durable_object.proto", import.meta.url)
const outputUrl = new URL("../workflow/generated/actorGrpcSchema.ts", import.meta.url)
const source = await readFile(protoUrl, "utf8")
const { root } = protobuf.parse(source, { keepCase: false })
const schema = JSON.stringify(root.toJSON(), (key, value) => (key === "protoName" ? undefined : value), 4)
    .replace(/^(\s*)"([A-Za-z_$][\w$]*)":/gmu, "$1$2:")
    .replace(/oneof: \[\n([\s\S]*?)\n\s*\]/gu, (_match, items) => `oneof: [${items.trim().replace(/\s+/gu, " ")}]`)
const output = `// Generated from proto/durable_object.proto. Do not edit by hand.\nimport type { INamespace } from "protobufjs"\n\nconst actorGrpcSchema: INamespace = ${schema}\n\nexport { actorGrpcSchema }\n`

await writeFile(outputUrl, output)
