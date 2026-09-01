import { access, readFile } from "node:fs/promises"

await checkPackage()

async function checkPackage() {
    const metadata = JSON.parse(await readFile("package.json", "utf8"))
    if (metadata.name !== "lil-durable-objects") throw new Error("unexpected package name")
    if (metadata.license !== "MIT") throw new Error("package license must be MIT")
    for (const entry of Object.values(metadata.exports)) await checkExport(entry)
    for (const path of Object.values(metadata.bin)) await access(path)
    await Promise.all(metadata.files.filter(path => !path.includes("dist")).map(path => access(path)))
}

async function checkExport(entry) {
    if (typeof entry !== "object" || entry === null) throw new Error("package exports must declare import and types paths")
    await Promise.all([access(entry.import), access(entry.types)])
}
