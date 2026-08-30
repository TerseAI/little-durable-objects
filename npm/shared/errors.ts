class ActorConfigurationError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options)
        this.name = "ActorConfigurationError"
    }
}

class ActorDefinitionError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "ActorDefinitionError"
    }
}

class ActorInvocationError extends Error {
    readonly code: string
    readonly requestId: string

    constructor(code: string, requestId: string, message: string) {
        super(message)
        this.name = "ActorInvocationError"
        this.code = code
        this.requestId = requestId
    }
}

class ActorSessionError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options)
        this.name = "ActorSessionError"
    }
}

class ActorProtocolError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options)
        this.name = "ActorProtocolError"
    }
}

class ActorSerializationError extends Error {
    constructor(message: string, options?: ErrorOptions) {
        super(message, options)
        this.name = "ActorSerializationError"
    }
}

class ActorValidationError extends Error {
    constructor(message: string) {
        super(message)
        this.name = "ActorValidationError"
    }
}

export { ActorConfigurationError, ActorDefinitionError, ActorInvocationError, ActorProtocolError, ActorSerializationError, ActorSessionError, ActorValidationError }
