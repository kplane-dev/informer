package informer

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/storage"
)

// ClusterIndexName is the name of the indexer that indexes objects by cluster ID.
const ClusterIndexName = "mc.cluster"

// CompositeKeyFunc generates a store key that is unique across clusters.
// Keys have the format "clusterID/namespace/name" for namespaced resources
// or "clusterID/name" for cluster-scoped resources.
//
// The function handles ObjectWithClusterIdentity objects and
// cache.DeletedFinalStateUnknown tombstones.
func CompositeKeyFunc(obj interface{}) (string, error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	env, ok := obj.(*mcstorage.ObjectWithClusterIdentity)
	if !ok {
		return "", fmt.Errorf("expected *ObjectWithClusterIdentity, got %T", obj)
	}
	accessor, err := meta.Accessor(env.Object)
	if err != nil {
		return "", fmt.Errorf("cannot access object metadata: %w", err)
	}
	return CompositeKey(env.ClusterID, accessor.GetNamespace(), accessor.GetName()), nil
}

// CompositeKey builds a composite key from cluster ID, namespace, and name.
func CompositeKey(clusterID, namespace, name string) string {
	if namespace == "" {
		return clusterID + "/" + name
	}
	return clusterID + "/" + namespace + "/" + name
}

// ClusterIndexFunc extracts the cluster ID from an object for indexing.
func ClusterIndexFunc(obj interface{}) ([]string, error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	env, ok := obj.(*mcstorage.ObjectWithClusterIdentity)
	if !ok || env.ClusterID == "" {
		return nil, nil
	}
	return []string{env.ClusterID}, nil
}

// CompositeStore is a cluster-indexed object store. It stores
// *ObjectWithClusterIdentity objects keyed by clusterID/namespace/name,
// and supports efficient per-cluster queries via the cluster index.
type CompositeStore struct {
	indexer cache.Indexer
}

// NewCompositeStore creates a new CompositeStore.
func NewCompositeStore() *CompositeStore {
	return &CompositeStore{
		indexer: cache.NewIndexer(CompositeKeyFunc, cache.Indexers{
			ClusterIndexName: ClusterIndexFunc,
		}),
	}
}

// Add adds an object to the store.
func (s *CompositeStore) Add(obj *mcstorage.ObjectWithClusterIdentity) error {
	return s.indexer.Add(obj)
}

// Update replaces an object in the store.
func (s *CompositeStore) Update(obj *mcstorage.ObjectWithClusterIdentity) error {
	return s.indexer.Update(obj)
}

// Delete removes an object from the store.
func (s *CompositeStore) Delete(obj *mcstorage.ObjectWithClusterIdentity) error {
	return s.indexer.Delete(obj)
}

// GetByKey looks up an object by its composite key (clusterID/namespace/name).
func (s *CompositeStore) GetByKey(key string) (*mcstorage.ObjectWithClusterIdentity, bool, error) {
	item, exists, err := s.indexer.GetByKey(key)
	if err != nil || !exists {
		return nil, exists, err
	}
	env, ok := item.(*mcstorage.ObjectWithClusterIdentity)
	if !ok {
		return nil, false, fmt.Errorf("unexpected type %T in store", item)
	}
	return env, true, nil
}

// Get looks up an object by cluster ID, namespace, and name.
func (s *CompositeStore) Get(clusterID, namespace, name string) (*mcstorage.ObjectWithClusterIdentity, bool, error) {
	return s.GetByKey(CompositeKey(clusterID, namespace, name))
}

// ListByCluster returns all unwrapped objects for a specific cluster.
// Objects are unwrapped from any cacher-internal wrappers (e.g., cachingObject).
func (s *CompositeStore) ListByCluster(clusterID string) []runtime.Object {
	items, err := s.indexer.ByIndex(ClusterIndexName, clusterID)
	if err != nil {
		return nil
	}
	result := make([]runtime.Object, 0, len(items))
	for _, item := range items {
		if env, ok := item.(*mcstorage.ObjectWithClusterIdentity); ok {
			result = append(result, unwrapCacheableObject(env.Object))
		}
	}
	return result
}

// ListAll returns all unwrapped objects across all clusters.
// Objects are unwrapped from any cacher-internal wrappers (e.g., cachingObject).
func (s *CompositeStore) ListAll() []runtime.Object {
	items := s.indexer.List()
	result := make([]runtime.Object, 0, len(items))
	for _, item := range items {
		if env, ok := item.(*mcstorage.ObjectWithClusterIdentity); ok {
			result = append(result, unwrapCacheableObject(env.Object))
		}
	}
	return result
}

// unwrapCacheableObject returns the concrete object type from a runtime.Object.
// If the object implements runtime.CacheableObject (e.g., the cacher's internal
// cachingObject), GetObject() is called to obtain the real type.
func unwrapCacheableObject(obj runtime.Object) runtime.Object {
	if co, ok := obj.(runtime.CacheableObject); ok {
		return co.GetObject()
	}
	return obj
}

// Clusters returns the set of known cluster IDs.
func (s *CompositeStore) Clusters() []string {
	return s.indexer.ListIndexFuncValues(ClusterIndexName)
}

// Len returns the total number of objects in the store.
func (s *CompositeStore) Len() int {
	return len(s.indexer.List())
}

// Indexer returns the underlying cache.Indexer for advanced queries.
func (s *CompositeStore) Indexer() cache.Indexer {
	return s.indexer
}
