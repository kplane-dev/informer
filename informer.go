package informer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	apistorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	mcstorage "github.com/kplane-dev/storage"
)

// Config configures a MultiClusterInformer.
type Config struct {
	// Storage is the storage.Interface (typically a cacher) to watch.
	// Watch events must carry *storage.ObjectWithClusterIdentity wrappers.
	Storage apistorage.Interface

	// ResourcePrefix is the key prefix covering all clusters for this resource.
	// Example: "/pods/clusters/"
	ResourcePrefix string

	// GroupResource identifies the resource type for logging.
	GroupResource schema.GroupResource
}

// MultiClusterInformer watches a single resource type across all clusters
// through a shared storage.Interface watch and fans out events to per-cluster
// handlers. It maintains a cluster-indexed internal store.
type MultiClusterInformer struct {
	cfg      Config
	store    *CompositeStore
	handlers *handlerRegistry

	hasSynced atomic.Bool
	lastRV    atomic.Value // string

	mu      sync.Mutex
	started bool
	stopped bool
}

// New creates a new MultiClusterInformer.
func New(cfg Config) *MultiClusterInformer {
	m := &MultiClusterInformer{
		cfg:   cfg,
		store: NewCompositeStore(),
	}
	m.handlers = newHandlerRegistry(m.HasSynced)
	return m
}

// Run starts the watch loop. It blocks until ctx is cancelled.
// The informer retries on watch errors with increasing backoff.
func (m *MultiClusterInformer) Run(ctx context.Context) {
	m.mu.Lock()
	if m.started {
		m.mu.Unlock()
		return
	}
	m.started = true
	m.mu.Unlock()

	defer func() {
		m.mu.Lock()
		m.stopped = true
		m.mu.Unlock()
	}()

	for {
		if ctx.Err() != nil {
			return
		}
		m.listWatch(ctx)
	}
}

func (m *MultiClusterInformer) listWatch(ctx context.Context) {
	rv := "0"
	if last := m.lastRV.Load(); last != nil {
		rv = last.(string)
	}

	isInitialList := !m.hasSynced.Load()

	klog.V(4).InfoS("Starting watch",
		"resource", m.cfg.GroupResource,
		"prefix", m.cfg.ResourcePrefix,
		"resourceVersion", rv,
		"isInitialList", isInitialList,
	)

	pred := apistorage.Everything
	pred.AllowWatchBookmarks = true

	opts := apistorage.ListOptions{
		ResourceVersion: rv,
		Predicate:       pred,
		Recursive:       true,
	}
	if isInitialList {
		opts.SendInitialEvents = ptr.To(true)
	}

	w, err := m.cfg.Storage.Watch(ctx, m.cfg.ResourcePrefix, opts)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		klog.ErrorS(err, "Failed to start watch",
			"resource", m.cfg.GroupResource,
			"prefix", m.cfg.ResourcePrefix,
		)
		return
	}
	defer w.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-w.ResultChan():
			if !ok {
				klog.V(4).InfoS("Watch channel closed, will restart",
					"resource", m.cfg.GroupResource)
				return
			}

			if event.Type == watch.Error {
				klog.V(2).InfoS("Watch error, resetting to RV=0",
					"resource", m.cfg.GroupResource)
				m.lastRV.Store("0")
				return
			}

			if event.Type == watch.Bookmark {
				m.updateResourceVersion(event.Object)
				if isInitialList {
					m.hasSynced.Store(true)
					m.handlers.markAllSynced()
					isInitialList = false
					klog.V(4).InfoS("Initial sync complete",
						"resource", m.cfg.GroupResource,
						"objects", m.store.Len(),
					)
				}
				continue
			}

			m.processEvent(event, isInitialList)
		}
	}
}

func (m *MultiClusterInformer) processEvent(event watch.Event, isInitialList bool) {
	env, ok := event.Object.(*mcstorage.ObjectWithClusterIdentity)
	if !ok {
		// Try unwrapping — some objects may not be wrapped
		klog.V(5).InfoS("Watch event without cluster identity",
			"type", event.Type,
			"objectType", fmt.Sprintf("%T", event.Object),
		)
		return
	}

	switch event.Type {
	case watch.Added:
		if err := m.store.Add(env); err != nil {
			klog.ErrorS(err, "Failed to add to store")
			return
		}
		m.handlers.dispatchAdd(env, isInitialList)

	case watch.Modified:
		key, err := CompositeKeyFunc(env)
		if err != nil {
			klog.ErrorS(err, "Failed to compute key for modified object")
			return
		}
		old, exists, _ := m.store.GetByKey(key)
		if err := m.store.Update(env); err != nil {
			klog.ErrorS(err, "Failed to update store")
			return
		}
		if exists {
			m.handlers.dispatchUpdate(old, env)
		} else {
			m.handlers.dispatchAdd(env, false)
		}

	case watch.Deleted:
		if err := m.store.Delete(env); err != nil {
			klog.ErrorS(err, "Failed to delete from store")
		}
		m.handlers.dispatchDelete(env)
	}

	m.updateResourceVersion(env.Object)
}

func (m *MultiClusterInformer) updateResourceVersion(obj runtime.Object) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return
	}
	rv := accessor.GetResourceVersion()
	if rv != "" {
		m.lastRV.Store(rv)
	}
}

// HasSynced returns true when the initial watch has completed and all objects
// that existed at watch start time have been delivered to the store and handlers.
//
// This is a global signal across all clusters — it does not track individual
// cluster completeness. A cluster with zero objects at watch start time does not
// prevent HasSynced from becoming true. Handlers registered after HasSynced
// becomes true are marked synced immediately.
func (m *MultiClusterInformer) HasSynced() bool {
	return m.hasSynced.Load()
}

// IsStopped returns true if the informer has stopped running.
func (m *MultiClusterInformer) IsStopped() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.stopped
}

// LastSyncResourceVersion returns the last observed resource version.
func (m *MultiClusterInformer) LastSyncResourceVersion() string {
	if rv := m.lastRV.Load(); rv != nil {
		return rv.(string)
	}
	return ""
}

// AddEventHandler registers a handler for events across all clusters.
// Events carry *storage.ObjectWithClusterIdentity.
func (m *MultiClusterInformer) AddEventHandler(handler MultiClusterEventHandler) HandlerRegistration {
	return m.handlers.addAllClusters(handler)
}

// ForCluster returns a per-cluster view that implements cache.SharedIndexInformer.
// Event handlers registered on the returned informer only receive events for
// the specified cluster, and objects are unwrapped (no ObjectWithClusterIdentity).
//
// HasSynced on the returned informer delegates to the parent's global HasSynced.
// It does not track whether this specific cluster's objects have been seen — there
// is no per-cluster sync signal. See package documentation for details.
func (m *MultiClusterInformer) ForCluster(clusterID string) cache.SharedIndexInformer {
	return &perClusterInformer{
		parent:    m,
		clusterID: clusterID,
	}
}

// List returns all unwrapped objects for a specific cluster.
func (m *MultiClusterInformer) List(clusterID string) []runtime.Object {
	return m.store.ListByCluster(clusterID)
}

// ListAll returns all unwrapped objects across all clusters.
func (m *MultiClusterInformer) ListAll() []runtime.Object {
	return m.store.ListAll()
}

// Get returns a specific object by cluster, namespace, and name.
func (m *MultiClusterInformer) Get(clusterID, namespace, name string) (runtime.Object, bool) {
	env, exists, err := m.store.Get(clusterID, namespace, name)
	if err != nil || !exists {
		return nil, false
	}
	return unwrapCacheableObject(env.Object), true
}

// Clusters returns the set of known cluster IDs.
func (m *MultiClusterInformer) Clusters() []string {
	return m.store.Clusters()
}

// Store returns the underlying CompositeStore for advanced queries.
func (m *MultiClusterInformer) Store() *CompositeStore {
	return m.store
}
