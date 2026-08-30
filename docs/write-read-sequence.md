# Write and read sequence

## Mutation

```text
application        control plane          host               SQLite/LTX        durability
    | resolve ---------->|                 |                      |                  |
    |<---------- route ---|                |                      |                  |
    | invoke(id, set) -------------------->| lock + own            |                  |
    |                                     |--------------------->| transaction      |
    |                                     |<---------------------| captured LTX     |
    |                                     |--------------------------------------->| publish CAS
    |                                     |<---------------------------------------| manifest
    |                                     |--------------------->| cache watermark |
    |<-------------------------- completed|                      |                  |
```

The host acknowledges only after the immutable LTX bundle and manifest compare-and-swap are canonical. If the publication outcome cannot be proven, it returns `outcome_unknown`, invalidates the local cache marker, and restores before executing that object again.

## Read or receipt replay

```text
application        control plane          host                    local SQLite
    | resolve ---------->|                 |                           |
    |<---------- route ---|                |                           |
    | invoke(id, get) -------------------->| verify lease + ownership  |
    |                                     |-------------------------->| read
    |<-------------------------- completed|<--------------------------|
```

Even a read verifies the active lease before returning. A repeated request ID reads its durable receipt and returns the original outcome without rerunning JavaScript.

## Warm restart

On the first access in a replacement process, the host compares the volume's cache marker with the canonical manifest TXID and runs SQLite `quick_check`. A match skips object download; any mismatch, missing file, invalid marker, or integrity failure restores the checkpoint and LTX tail before execution.
