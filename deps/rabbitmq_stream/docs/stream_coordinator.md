# Stream Coordinator

## Single Active Consumer

### "Simple" SAC (Not Super Stream)

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant C1 as Connection 1
    participant C2 as Connection 2
    participant C3 as Connection 3
    Note over C,C3: Simple SAC (not super stream)
    C1->>C: register sub 1
    C-)C1: {sac, sub 1, active = true}
    activate C1
    C1->>C1: consumer update to client
    C2->>C: register sub 2
    C3->>C: register sub 3
    C1->>C: unregister sub 1
    deactivate C1
    C-)C2: {sac, sub 2, active = true}
    activate C2
    C2->>C2: consumer update to client
    deactivate C2
```

### SAC with Super Stream Partition

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant C1 as Connection 1
    participant C2 as Connection 2
    participant C3 as Connection 3
    Note over C,C3: Super Stream SAC (partition = 1)
    C1->>C: register sub 1
    C-)C1: {sac, sub 1, active = true}
    activate C1
    C2->>C: register sub 2
    C-)C1: {sac, sub 1, active = false, step down = true}
    deactivate C1
    C1->>C1: consumer update to client
    C1->>C: activate consumer in group
    C-)C2: {sac, sub 2, active = true}
    activate C2
    C2->>C2: consumer update to client
    C3->>C: register sub 3
    Note over C, C3: active consumer stays the same (partition % consumers = 1 % 3 = 1)
    deactivate C2
```

### `noconnection` management 

```mermaid
flowchart TB
  A(monitor) --noconnection--> B(status = disconnected, set up timer)
  B -. timeout .-> C(status = forgotten)
  B -. nodeup .-> D(reissue monitors, send msg to connections)
  D -. down .-> E(handle connection down)
  D -. connection response .-> F(evaluate impacted groups)
```

* composite status for consumers: `{connected, active}`, `{disconnected,active}`, etc.
* `disconnected` status can prevent rebalancing in a group, e.g. `{disconnected, active}` (it is impossible to tell the active consumer to step down)
* consumers in `forgotten` status are ignored during rebalancing
* it may be necessary to reconcile a group if a `{forgotten, active}` consumer comes back in a group ("evaluate impacted groups" box above).
This is unlikely though.

### Stale Node Detection

```mermaid
flowchart TB
  A(RA) -- tick --> B(stale nodes = RA known nodes - cluster nodes)
  B -. no stale nodes .-> C(nothing to do)
  B -. stale nodes .-> D(remove connections from state)
```
