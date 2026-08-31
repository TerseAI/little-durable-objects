# System architecture

```text
trusted backend -- admin env token --> control plane
       |                                  |
       | project JWT                      | ensure one regional host
       v                                  v
workflow -- Invoke(project JWT) --> control plane -- Invoke(host JWT) --> actor host
                                      |                               |
                                      | ownership + lease             | signed GET/conditional PUT
                                      v                               v
                                  Postgres                    regional GCS bucket
                                                                    NDJSON state
```

The workflow talks only to the control plane. The control plane selects the host and issues short-lived, object-specific storage URLs. Actor code receives neither the admin credential nor GCP credentials.
