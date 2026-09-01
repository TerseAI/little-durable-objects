# System architecture

```text
trusted backend -- REST/admin token --> control plane
       |                                      |
       | project JWT                          | ensure one regional host
       v                                      v
workflow -- REST/project JWT ----------> control plane -- gRPC/host JWT --> actor host
                                          |                              |
                                          | ownership + lease            | signed GET/conditional PUT
                                          v                              v
                                      Postgres                   regional GCS bucket
                                                                       NDJSON state
```

The public integration surface is HTTP/JSON. gRPC is reserved for authenticated internal traffic between the control plane and actor hosts. The workflow talks only to the control plane, which selects the host and issues short-lived, object-specific storage URLs. Actor code receives neither the admin credential nor GCP credentials.
