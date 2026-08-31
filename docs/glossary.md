# Glossary

- **Durable object:** Stateful code addressed by a stable object identity.
- **Actor API:** The existing public TypeScript programming model built around
  `Actor`, `Actor.get(id)`, and typed method calls. It is a compatibility
  boundary for the minimal-runtime refactor.
- **Object identity:** The tuple `namespace / type / id`.
- **Activation:** Loading an object's state and code into memory.
- **Idle eviction:** Removing an inactive object from memory after a timeout.
- **Reactivation:** Loading an evicted object's durable state again.
- **State log:** The NDJSON data stored for one object.
- **State record:** One committed NDJSON line containing the object's complete
  serialized state after a successful mutation.
- **Compaction:** Rewriting a state log into a smaller equivalent form.
- **Host lease:** A short-lived Postgres record proving that a sandbox host is
  still eligible to own and execute objects.
- **Owner epoch:** A number increased whenever an object changes owners; stale
  sandbox responses cannot commit under an older epoch.
- **Signed storage URL:** A short-lived capability for one host to read or
  conditionally replace one object's state blob without receiving GCP
  credentials.
- **Home region:** The sandbox region assigned to an object and reused when it
  is reactivated or moved to a replacement host. It also selects the object's
  nearby Standard bucket.
- **Ambiguous outcome:** A failed or timed-out invocation that may already have
  committed its state even though the caller did not receive success.
- **Control plane:** The API that authenticates workflows and coordinates
  object execution.
- **Sandbox provider:** The implementation that starts isolated actor hosts;
  Modal is the only implementation today.
