const canonicalRegionPattern = /^[a-z0-9](?:[a-z0-9._-]{0,62}[a-z0-9])?$/u

interface ModalPlacement {
    readonly regions: readonly string[]
    readonly cloud?: string
    readonly observedPlacements: readonly string[]
}

interface RegionDefinition {
    readonly modal: ModalPlacement
}

type CanonicalRegionCatalog = Readonly<Record<string, RegionDefinition>>

const recommendedRegionCatalog = {
    "north-america-east": {
        modal: { regions: ["us-east"], cloud: "gcp", observedPlacements: ["gcp:us-east*"] }
    },
    "north-america-central": {
        modal: { regions: ["us-central"], cloud: "gcp", observedPlacements: ["gcp:us-central*"] }
    },
    "north-america-south": {
        modal: { regions: ["us-south"], cloud: "gcp", observedPlacements: ["gcp:us-south*"] }
    },
    "north-america-west": {
        modal: { regions: ["us-west"], cloud: "gcp", observedPlacements: ["gcp:us-west*"] }
    },
    "europe-west": {
        modal: { regions: ["eu-west"], cloud: "gcp", observedPlacements: ["gcp:europe-west*"] }
    },
    "asia-southeast": {
        modal: { regions: ["ap-southeast"], cloud: "gcp", observedPlacements: ["gcp:asia-southeast*"] }
    }
} as const satisfies CanonicalRegionCatalog

function validateCanonicalRegion(value: string): string {
    if (!canonicalRegionPattern.test(value)) throw new Error(`invalid canonical region ${JSON.stringify(value)}`)
    return value
}

function modalPlacement(region: string, catalog: CanonicalRegionCatalog = recommendedRegionCatalog): ModalPlacement {
    const placement = catalog[validateCanonicalRegion(region)]?.modal
    if (!placement) throw new Error(`canonical region ${JSON.stringify(region)} has no Modal placement`)
    return placement
}

function canonicalRegionForModal(cloud: string | undefined, region: string | undefined, catalog: CanonicalRegionCatalog = recommendedRegionCatalog): string | undefined {
    if (!region) return undefined
    const normalizedRegion = region.toLowerCase()
    const candidates = [cloud ? `${cloud.toLowerCase()}:${normalizedRegion}` : undefined, normalizedRegion].filter((candidate): candidate is string => candidate !== undefined)
    return Object.entries(catalog).find(([, definition]) => definition.modal.observedPlacements.some(pattern => candidates.some(candidate => matchesPlacement(pattern, candidate))))?.[0]
}

function matchesPlacement(pattern: string, candidate: string): boolean {
    const normalized = pattern.toLowerCase()
    return normalized.endsWith("*") ? candidate.startsWith(normalized.slice(0, -1)) : candidate === normalized
}

export { canonicalRegionForModal, modalPlacement, recommendedRegionCatalog, validateCanonicalRegion }
export type { CanonicalRegionCatalog, ModalPlacement, RegionDefinition }
