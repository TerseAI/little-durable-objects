import { actorClient } from "../workflow/client.js"

import { ActorDefinitionError } from "./errors.js"
import { validateActorComponent } from "./types.js"

const actorMetadata = new WeakMap<object, ActorMetadata>()
const actorDefinitions = new Map<string, ActorDefinition>()
const asyncFunction = Object.getPrototypeOf(async () => {}).constructor
const referenceClasses = new WeakMap<Function, ActorReferenceClass>()

abstract class Actor {
    protected constructor() {}

    static get<TActorClass extends ActorClass>(this: ValidActorClass<TActorClass>, actorId: string): ActorReference<TActorClass["prototype"]> {
        return getActorReference(this, validateActorComponent("actor ID", actorId))
    }

    protected get id(): string {
        return metadataFor(this).actorId
    }
}

function registerActorClass<Instance extends Actor>(actorClass: ActorClass<Instance>): ActorDefinition {
    const actorType = actorName(actorClass)
    const existing = actorDefinitions.get(actorType)
    if (existing !== undefined) {
        if (existing.actorClass !== actorClass) throw new ActorDefinitionError(`duplicate actor type ${actorType}`)
        return existing
    }

    validateActorClass(actorClass, actorType)
    const definition = {
        actorType: validateActorComponent("actor type", actorType),
        actorClass,
        methods: new Set(discoverMethods(actorClass, actorType))
    }
    actorDefinitions.set(actorType, definition)
    return definition
}

function findActorDefinition(actorType: string): ActorDefinition | undefined {
    return actorDefinitions.get(actorType)
}

function getActorReference<TActorClass extends ActorClass>(actorClass: TActorClass, actorId: string): ActorReference<TActorClass["prototype"]> {
    const definition = registerActorClass(actorClass)
    const Reference = referenceClass(definition)
    return new Reference(actorId) as unknown as ActorReference<TActorClass["prototype"]>
}

function referenceClass(definition: ActorDefinition): ActorReferenceClass {
    const existing = referenceClasses.get(definition.actorClass)
    if (existing !== undefined) return existing

    class ActorReference extends Actor {
        constructor(actorId: string) {
            super()
            bindActorIdentity(this, actorId)
        }
    }

    definition.methods.forEach(method => {
        Object.defineProperty(ActorReference.prototype, method, {
            configurable: false,
            enumerable: false,
            writable: false,
            value: function forwardActorMethod(this: Actor, ...args: unknown[]): Promise<unknown> {
                const metadata = metadataFor(this)
                return actorClient().invoke(definition.actorType, metadata.actorId, method, args)
            }
        })
    })
    referenceClasses.set(definition.actorClass, ActorReference)
    return ActorReference
}

function bindActorIdentity(instance: Actor, actorId: string): void {
    actorMetadata.set(instance, { actorId: validateActorComponent("actor ID", actorId) })
}

function metadataFor(instance: Actor): ActorMetadata {
    const metadata = actorMetadata.get(instance)
    if (metadata === undefined) throw new ActorDefinitionError("actor identity is unavailable outside an actor invocation")
    return metadata
}

function discoverMethods(actorClass: ActorClass, actorType: string): string[] {
    if (Object.getOwnPropertySymbols(actorClass.prototype).length > 0) throw new ActorDefinitionError(`actor class ${actorType} cannot define symbol methods`)

    return Object.entries(Object.getOwnPropertyDescriptors(actorClass.prototype)).flatMap(([name, descriptor]) => {
        if (name === "constructor") return []
        if (descriptor.get !== undefined || descriptor.set !== undefined) throw new ActorDefinitionError(`actor class ${actorType} cannot define accessor ${name}`)
        if (typeof descriptor.value !== "function") return []
        validateActorComponent("actor method", name)
        if (name === "then") throw new ActorDefinitionError(`actor class ${actorType} cannot define method then`)
        if (!(descriptor.value instanceof asyncFunction)) throw new ActorDefinitionError(`actor method ${actorType}.${name} must be async`)
        return [name]
    })
}

function validateActorClass(actorClass: ActorClass, actorType: string): void {
    if (Object.getPrototypeOf(actorClass.prototype) !== Actor.prototype) throw new ActorDefinitionError(`actor class ${actorType} must extend Actor directly`)
    if (actorClass.length !== 0) throw new ActorDefinitionError(`actor class ${actorType} cannot require constructor arguments`)
}

function actorName(actorClass: ActorClass): string {
    if (actorClass.name.length === 0) throw new ActorDefinitionError("actor classes must be named")
    return actorClass.name
}

interface ActorDefinition {
    readonly actorType: string
    readonly actorClass: ActorClass
    readonly methods: ReadonlySet<string>
}

interface ActorMetadata {
    readonly actorId: string
}

type ActorClass<Instance extends Actor = Actor> = Function & {
    readonly prototype: Instance
}
type ActorReferenceClass = new (actorId: string) => Actor
type AsyncMethod = (...args: never[]) => Promise<unknown>
type PubliclyConstructibleActorClass = abstract new (...args: never[]) => Actor
type InvalidActorMethod<Instance extends Actor> = {
    [Key in keyof Instance]: Instance[Key] extends (...args: never[]) => unknown ? (Instance[Key] extends AsyncMethod ? never : Key) : never
}[keyof Instance]
type ValidActorClass<TActorClass extends ActorClass> = TActorClass extends PubliclyConstructibleActorClass ? never : InvalidActorMethod<TActorClass["prototype"]> extends never ? TActorClass : never
type ActorReference<Instance extends Actor> = {
    [Key in keyof Instance as Instance[Key] extends AsyncMethod ? Key : never]: Instance[Key]
}

export { Actor, bindActorIdentity, findActorDefinition, registerActorClass }
export type { ActorClass, ActorDefinition, ActorReference }
