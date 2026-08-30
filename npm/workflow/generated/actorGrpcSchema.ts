// Generated from proto/durable_object.proto. Do not edit by hand.
import type { INamespace } from "protobufjs"

const actorGrpcSchema: INamespace = {
    nested: {
        durable_object: {
            nested: {
                v1: {
                    nested: {
                        ActorHostService: {
                            methods: {
                                Invoke: {
                                    requestType: "InvokeActorRequest",
                                    responseType: "InvokeActorReply"
                                }
                            }
                        },
                        ActorControlPlaneService: {
                            methods: {
                                ResolveActorHost: {
                                    requestType: "ResolveActorHostRequest",
                                    responseType: "ResolvedActorHost"
                                },
                                Execute: {
                                    requestType: "ControlPlaneRequest",
                                    responseType: "ControlPlaneReply"
                                }
                            }
                        },
                        ResolveActorHostRequest: {
                            fields: {
                                actor: {
                                    type: "ActorKey",
                                    id: 1
                                }
                            }
                        },
                        ResolvedActorHost: {
                            fields: {
                                route: {
                                    type: "string",
                                    id: 1
                                }
                            }
                        },
                        ControlPlaneRequest: {
                            fields: {
                                commandJson: {
                                    type: "bytes",
                                    id: 1
                                },
                                binaryPayloads: {
                                    rule: "repeated",
                                    type: "bytes",
                                    id: 2
                                }
                            }
                        },
                        ControlPlaneReply: {
                            fields: {
                                replyJson: {
                                    type: "bytes",
                                    id: 1
                                },
                                binaryPayloads: {
                                    rule: "repeated",
                                    type: "bytes",
                                    id: 2
                                }
                            }
                        },
                        ActorKey: {
                            fields: {
                                namespaceId: {
                                    type: "string",
                                    id: 1
                                },
                                actorType: {
                                    type: "string",
                                    id: 2
                                },
                                actorId: {
                                    type: "string",
                                    id: 3
                                }
                            }
                        },
                        InvokeActorRequest: {
                            fields: {
                                requestId: {
                                    type: "string",
                                    id: 1
                                },
                                actor: {
                                    type: "ActorKey",
                                    id: 2
                                },
                                method: {
                                    type: "string",
                                    id: 3
                                },
                                argsJson: {
                                    type: "bytes",
                                    id: 4
                                },
                                timeoutMs: {
                                    type: "uint64",
                                    id: 5
                                }
                            }
                        },
                        InvokeActorReply: {
                            oneofs: {
                                result: {
                                    oneof: ["completed", "failed", "reroute"]
                                }
                            },
                            fields: {
                                completed: {
                                    type: "ActorCompleted",
                                    id: 1
                                },
                                failed: {
                                    type: "ActorFailed",
                                    id: 2
                                },
                                reroute: {
                                    type: "Reroute",
                                    id: 3
                                }
                            }
                        },
                        ActorCompleted: {
                            fields: {
                                resultJson: {
                                    type: "bytes",
                                    id: 1
                                }
                            }
                        },
                        ActorFailed: {
                            fields: {
                                code: {
                                    type: "string",
                                    id: 1
                                },
                                message: {
                                    type: "string",
                                    id: 2
                                }
                            }
                        },
                        Reroute: {
                            fields: {}
                        }
                    }
                }
            }
        }
    }
}

export { actorGrpcSchema }
