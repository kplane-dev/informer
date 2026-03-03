package informer

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/storage"
)

// perClusterInformer implements cache.SharedIndexInformer scoped to one cluster.
// It is a filtered view over the parent MultiClusterInformer.
type perClusterInformer struct {
	parent    *MultiClusterInformer
	clusterID string
}

var _ cache.SharedIndexInformer = (*perClusterInformer)(nil)

// AddEventHandler registers a handler that receives events only for this cluster.
// Objects passed to the handler are unwrapped (no ObjectWithClusterIdentity).
func (p *perClusterInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	reg := p.parent.handlers.addForCluster(p.clusterID, handler)
	return reg, nil
}

func (p *perClusterInformer) AddEventHandlerWithResyncPeriod(handler cache.ResourceEventHandler, _ time.Duration) (cache.ResourceEventHandlerRegistration, error) {
	return p.AddEventHandler(handler)
}

func (p *perClusterInformer) AddEventHandlerWithOptions(handler cache.ResourceEventHandler, _ cache.HandlerOptions) (cache.ResourceEventHandlerRegistration, error) {
	return p.AddEventHandler(handler)
}

func (p *perClusterInformer) RemoveEventHandler(handle cache.ResourceEventHandlerRegistration) error {
	if reg, ok := handle.(interface{ Remove() error }); ok {
		return reg.Remove()
	}
	return fmt.Errorf("unknown handler registration type: %T", handle)
}

// GetStore returns a filtered store view containing only objects for this cluster.
// Returned objects are unwrapped.
func (p *perClusterInformer) GetStore() cache.Store {
	return &perClusterStore{
		parent:    p.parent,
		clusterID: p.clusterID,
	}
}

// GetIndexer returns a filtered indexer view for this cluster.
func (p *perClusterInformer) GetIndexer() cache.Indexer {
	return &perClusterIndexer{
		perClusterStore: perClusterStore{
			parent:    p.parent,
			clusterID: p.clusterID,
		},
	}
}

// Run is a no-op — the parent MultiClusterInformer drives the watch.
func (p *perClusterInformer) Run(_ <-chan struct{}) {}

// RunWithContext is a no-op — the parent MultiClusterInformer drives the watch.
func (p *perClusterInformer) RunWithContext(_ context.Context) {}

// HasSynced delegates to the parent informer's global HasSynced. It does not
// track per-cluster sync — it reflects whether the shared watch across all
// clusters has completed its initial replay.
func (p *perClusterInformer) HasSynced() bool { return p.parent.HasSynced() }
func (p *perClusterInformer) HasSyncedChecker() cache.DoneChecker {
	return &doneChecker{
		name:   fmt.Sprintf("multicluster-informer/%s/%s", p.parent.cfg.GroupResource, p.clusterID),
		parent: p.parent,
	}
}
func (p *perClusterInformer) LastSyncResourceVersion() string {
	return p.parent.LastSyncResourceVersion()
}
func (p *perClusterInformer) GetController() cache.Controller { return nil }
func (p *perClusterInformer) IsStopped() bool                 { return p.parent.IsStopped() }

func (p *perClusterInformer) SetWatchErrorHandler(_ cache.WatchErrorHandler) error {
	return nil
}

func (p *perClusterInformer) SetWatchErrorHandlerWithContext(_ cache.WatchErrorHandlerWithContext) error {
	return nil
}

func (p *perClusterInformer) SetTransform(_ cache.TransformFunc) error {
	return fmt.Errorf("transforms are not supported on per-cluster informers")
}

func (p *perClusterInformer) AddIndexers(_ cache.Indexers) error {
	return fmt.Errorf("custom indexers are not supported on per-cluster informers")
}

// doneChecker implements cache.DoneChecker for the MultiClusterInformer.
type doneChecker struct {
	name   string
	parent *MultiClusterInformer
}

func (d *doneChecker) Name() string { return d.name }

func (d *doneChecker) Done() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for !d.parent.HasSynced() {
			// Spin briefly — this is called infrequently
			time.Sleep(10 * time.Millisecond)
		}
		close(ch)
	}()
	return ch
}

// perClusterStore provides a cache.Store view filtered to one cluster.
// Returned objects are unwrapped (inner runtime.Object, not ObjectWithClusterIdentity).
type perClusterStore struct {
	parent    *MultiClusterInformer
	clusterID string
}

func (s *perClusterStore) Add(obj interface{}) error    { return fmt.Errorf("read-only store") }
func (s *perClusterStore) Update(obj interface{}) error { return fmt.Errorf("read-only store") }
func (s *perClusterStore) Delete(obj interface{}) error { return fmt.Errorf("read-only store") }
func (s *perClusterStore) Replace(list []interface{}, resourceVersion string) error {
	return fmt.Errorf("read-only store")
}
func (s *perClusterStore) Resync() error { return nil }

func (s *perClusterStore) Bookmark(_ string) {}

func (s *perClusterStore) LastStoreSyncResourceVersion() string {
	return s.parent.LastSyncResourceVersion()
}

func (s *perClusterStore) List() []interface{} {
	items := s.parent.store.ListByCluster(s.clusterID)
	result := make([]interface{}, len(items))
	for i, item := range items {
		result[i] = item
	}
	return result
}

func (s *perClusterStore) ListKeys() []string {
	items, err := s.parent.store.indexer.ByIndex(ClusterIndexName, s.clusterID)
	if err != nil {
		return nil
	}
	keys := make([]string, 0, len(items))
	for _, item := range items {
		if env, ok := item.(*mcstorage.ObjectWithClusterIdentity); ok {
			ns := env.GetNamespace()
			name := env.GetName()
			if ns == "" {
				keys = append(keys, name)
			} else {
				keys = append(keys, ns+"/"+name)
			}
		}
	}
	return keys
}

func (s *perClusterStore) Get(obj interface{}) (interface{}, bool, error) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		return nil, false, err
	}
	return s.GetByKey(key)
}

// GetByKey expects "namespace/name" format and translates to the composite key.
func (s *perClusterStore) GetByKey(key string) (interface{}, bool, error) {
	compositeKey := s.clusterID + "/" + key
	env, exists, err := s.parent.store.GetByKey(compositeKey)
	if err != nil || !exists {
		return nil, exists, err
	}
	return unwrapCacheableObject(env.Object), true, nil
}

// perClusterIndexer extends perClusterStore with Indexer methods.
type perClusterIndexer struct {
	perClusterStore
}

func (idx *perClusterIndexer) Index(_ string, _ interface{}) ([]interface{}, error) {
	return nil, fmt.Errorf("Index not supported on per-cluster indexer")
}

func (idx *perClusterIndexer) IndexKeys(_ string, _ string) ([]string, error) {
	return nil, fmt.Errorf("IndexKeys not supported on per-cluster indexer")
}

func (idx *perClusterIndexer) ListIndexFuncValues(_ string) []string {
	return nil
}

func (idx *perClusterIndexer) ByIndex(_ string, _ string) ([]interface{}, error) {
	return nil, fmt.Errorf("ByIndex not supported on per-cluster indexer")
}

func (idx *perClusterIndexer) GetIndexers() cache.Indexers {
	return cache.Indexers{}
}

func (idx *perClusterIndexer) AddIndexers(_ cache.Indexers) error {
	return fmt.Errorf("custom indexers are not supported on per-cluster indexers")
}
