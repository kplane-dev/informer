# kplane-dev/informer

Multi-cluster informer that watches a single resource type across all clusters through a shared `storage.Interface` and fans out events to per-cluster handlers.

## Why Not Standard client-go Informers?

Three problems prevent using `cache.SharedIndexInformer` directly for multi-cluster watches:

1. **Reflector incompatibility** — `cache.Reflector` calls `meta.Accessor(event.Object)` on every watch event. `ObjectWithClusterIdentity` requires `metav1.Object` delegation to work with this.
2. **Key collisions** — `cache.MetaNamespaceKeyFunc` produces `namespace/name`, which collides when cluster c1 and c2 both have `default/nginx`.
3. **List identity gap** — `storage.Interface.GetList()` returns unwrapped objects — cluster identity is lost.

## Approach

Uses `storage.Interface.Watch(RV="0", SendInitialEvents=true)` directly, which replays all cached objects as ADDED events with `ObjectWithClusterIdentity` wrappers. This unifies initial sync and live watch into one path, sidestepping all three problems.

## Usage

```go
mci := informer.New(informer.Config{
    Storage:        delegator,         // storage.Interface (typically a CacheDelegator)
    ResourcePrefix: "/pods/clusters/",
    GroupResource:  schema.GroupResource{Resource: "pods"},
})

// Start the watch loop
ctx, cancel := context.WithCancel(context.Background())
go mci.Run(ctx)

// Wait for initial sync
cache.WaitForCacheSync(ctx.Done(), mci.HasSynced)
```

### All-clusters handler

Receives `*ObjectWithClusterIdentity` envelopes with cluster identity intact:

```go
mci.AddEventHandler(informer.MultiClusterEventHandlerFuncs{
    AddFunc: func(obj *storage.ObjectWithClusterIdentity, isInInitialList bool) {
        fmt.Printf("cluster=%s name=%s\n", obj.ClusterID, obj.GetName())
    },
})
```

### Per-cluster handler

Returns a `cache.SharedIndexInformer` scoped to one cluster. Handlers receive unwrapped `runtime.Object`:

```go
c1Informer := mci.ForCluster("c1")
c1Informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
    AddFunc: func(obj interface{}) {
        pod := obj.(*v1.Pod)
        fmt.Println(pod.Name)
    },
})
```

### Querying the store

```go
// List objects for a specific cluster
pods := mci.List("c1")

// List across all clusters
allPods := mci.ListAll()

// Get a specific object
obj, found := mci.Get("c1", "default", "nginx")

// Get known cluster IDs
clusters := mci.Clusters()

// Per-cluster store (filtered view)
store := mci.ForCluster("c1").GetStore()
items := store.List()
obj, exists, err := store.GetByKey("default/nginx")
```

## Architecture

```
storage.Interface.Watch(RV=0, SendInitialEvents=true)
         │
         ▼
   MultiClusterInformer
         │
    ┌────┴────┐
    ▼         ▼
CompositeStore   handlerRegistry
(cluster-indexed)    │
                ┌────┴────┐
                ▼         ▼
        all-cluster    per-cluster
        handlers       handlers
        (wrapped)      (unwrapped)
```

- **CompositeStore** — `cache.Indexer` with composite keys (`clusterID/namespace/name`) and a cluster index for efficient per-cluster queries.
- **handlerRegistry** — Thread-safe handler dispatch with `sync.RWMutex`. All-cluster handlers receive `ObjectWithClusterIdentity`; per-cluster handlers receive unwrapped `runtime.Object`.
- **perClusterInformer** — Implements `cache.SharedIndexInformer` for drop-in compatibility. `Run()` is a no-op (the parent drives the watch). Store methods return filtered, unwrapped objects.

## Dependencies

- `github.com/kplane-dev/storage` — `ObjectWithClusterIdentity`, `KeyLayout`
- `k8s.io/apiserver` — `storage.Interface`
- `k8s.io/client-go` — `cache.SharedIndexInformer`, `cache.Indexer`
- `k8s.io/apimachinery` — `runtime.Object`, `meta.Accessor`

## Testing

```bash
# Unit tests (store, dispatch)
go test -run "TestComposite|TestClusterIndex|TestDispatch" -v

# E2E tests (real etcd + cacher)
go test -run "TestE2E" -v

# All tests
go test -v ./...
```
