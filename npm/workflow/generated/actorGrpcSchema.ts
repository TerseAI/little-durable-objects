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
                        ActorAdminService: {
                            methods: {
                                EnsureNamespace: {
                                    requestType: "EnsureNamespaceRequest",
                                    responseType: "EnsureNamespaceReply"
                                },
                                RegisterLaunchSpec: {
                                    requestType: "RegisterLaunchSpecRequest",
                                    responseType: "RegisterLaunchSpecReply"
                                },
                                IssueWorkflowToken: {
                                    requestType: "IssueWorkflowTokenRequest",
                                    responseType: "IssueWorkflowTokenReply"
                                },
                                GetJwks: {
                                    requestType: "GetJwksRequest",
                                    responseType: "GetJwksReply"
                                }
                            }
                        },
                        EnsureNamespaceRequest: {
                            fields: {
                                namespaceId: {
                                    type: "string",
                                    id: 1
                                }
                            }
                        },
                        EnsureNamespaceReply: {
                            fields: {
                                created: {
                                    type: "bool",
                                    id: 1
                                }
                            }
                        },
                        RegisterLaunchSpecRequest: {
                            oneofs: {
                                _actorEntrypoint: {
                                    oneof: ["actorEntrypoint"]
                                }
                            },
                            fields: {
                                namespaceId: {
                                    type: "string",
                                    id: 1
                                },
                                codeRevision: {
                                    type: "string",
                                    id: 2
                                },
                                imageRef: {
                                    type: "string",
                                    id: 3
                                },
                                workingDirectory: {
                                    type: "string",
                                    id: 4
                                },
                                actorEntrypoint: {
                                    type: "string",
                                    id: 5,
                                    options: {
                                        proto3_optional: true
                                    }
                                }
                            }
                        },
                        RegisterLaunchSpecReply: {
                            fields: {
                                created: {
                                    type: "bool",
                                    id: 1
                                }
                            }
                        },
                        IssueWorkflowTokenRequest: {
                            fields: {
                                namespaceId: {
                                    type: "string",
                                    id: 1
                                },
                                executionId: {
                                    type: "string",
                                    id: 2
                                },
                                codeRevision: {
                                    type: "string",
                                    id: 3
                                },
                                region: {
                                    type: "string",
                                    id: 4
                                },
                                deadlineUnixMs: {
                                    type: "int64",
                                    id: 5
                                }
                            }
                        },
                        IssueWorkflowTokenReply: {
                            fields: {
                                token: {
                                    type: "string",
                                    id: 1
                                },
                                expiresAtMs: {
                                    type: "int64",
                                    id: 2
                                }
                            }
                        },
                        GetJwksRequest: {
                            fields: {}
                        },
                        GetJwksReply: {
                            fields: {
                                jwksJson: {
                                    type: "bytes",
                                    id: 1
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
