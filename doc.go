// Package informer provides a multicluster informer that watches a single
// resource type across all clusters through a shared storage.Interface watch
// and fans out events to per-cluster handlers.
//
// The MultiClusterInformer uses the storage.Interface directly (not client-go's
// Reflector) to preserve cluster identity from the cacher's
// ObjectWithClusterIdentity wrappers. It maintains a cluster-indexed internal
// store and dispatches events to both all-cluster and per-cluster handlers.
//
// For consumers that require a standard cache.SharedIndexInformer interface,
// the ForCluster method returns a per-cluster filtered view.
//
// # Sync Semantics
//
// HasSynced returns true when the cacher has replayed all objects that existed
// at watch start time (signaled by the initial-events-end bookmark). This is a
// global signal — it means "the shared watch has completed its initial replay
// across all clusters," not "a specific cluster's objects are complete."
//
// There is no per-cluster sync tracking. ForCluster("c1").HasSynced() delegates
// to the parent informer's HasSynced and reflects the same global state. If a
// cluster has no objects in etcd when the watch starts, HasSynced still becomes
// true — an empty result set is a complete result set.
//
// Handlers registered after initial sync completes are marked synced immediately,
// since there is no "initial list" to replay for late registrations. This matches
// the behavior of standard client-go informers for late-added handlers.
package informer
