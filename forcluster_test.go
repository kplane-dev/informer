package informer

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/storage"
)

func TestForCluster_HasSynced_DelegatesToParent(t *testing.T) {
	mci := New(Config{})
	c1 := mci.ForCluster("c1")

	if c1.HasSynced() {
		t.Error("ForCluster(c1).HasSynced() = true before parent synced")
	}

	// Simulate sync
	mci.hasSynced.Store(true)

	if !c1.HasSynced() {
		t.Error("ForCluster(c1).HasSynced() = false after parent synced")
	}
}

func TestForCluster_HasSynced_NoPerClusterDistinction(t *testing.T) {
	mci := New(Config{})

	// Add objects only for c1
	mci.store.Add(&mcstorage.ObjectWithClusterIdentity{
		Object:    &simplePod{ObjectMeta: metav1.ObjectMeta{Name: "nginx", Namespace: "default"}},
		ClusterID: "c1",
	})

	mci.hasSynced.Store(true)

	// c2 has no objects but HasSynced should still be true —
	// it reflects the global watch state, not per-cluster completeness.
	c2 := mci.ForCluster("c2")
	if !c2.HasSynced() {
		t.Error("ForCluster(c2).HasSynced() = false, want true (global sync, not per-cluster)")
	}
}

func TestForCluster_HandlerRegistration_HasSynced_LateRegistration(t *testing.T) {
	mci := New(Config{})
	mci.hasSynced.Store(true)
	mci.handlers.markAllSynced()

	// Register a handler after sync is complete
	c1 := mci.ForCluster("c1")
	reg, err := c1.AddEventHandler(cache.ResourceEventHandlerFuncs{})
	if err != nil {
		t.Fatal(err)
	}

	if !reg.HasSynced() {
		t.Error("late-registered handler HasSynced() = false, want true")
	}
}

func TestForCluster_Store_List(t *testing.T) {
	mci := New(Config{})

	mci.store.Add(wrap("nginx", "default", "c1"))
	mci.store.Add(wrap("redis", "default", "c1"))
	mci.store.Add(wrap("coredns", "kube-system", "c2"))

	c1Items := mci.ForCluster("c1").GetStore().List()
	if len(c1Items) != 2 {
		t.Errorf("c1 List() = %d items, want 2", len(c1Items))
	}

	c2Items := mci.ForCluster("c2").GetStore().List()
	if len(c2Items) != 1 {
		t.Errorf("c2 List() = %d items, want 1", len(c2Items))
	}

	// Cluster with no objects returns empty
	c3Items := mci.ForCluster("c3").GetStore().List()
	if len(c3Items) != 0 {
		t.Errorf("c3 List() = %d items, want 0", len(c3Items))
	}
}

func TestForCluster_Store_GetByKey(t *testing.T) {
	mci := New(Config{})

	mci.store.Add(wrap("nginx", "default", "c1"))
	mci.store.Add(wrap("nginx", "default", "c2"))

	c1Store := mci.ForCluster("c1").GetStore()

	obj, exists, err := c1Store.GetByKey("default/nginx")
	if err != nil || !exists {
		t.Fatalf("c1 GetByKey(default/nginx): exists=%v err=%v", exists, err)
	}
	pod := obj.(*simplePod)
	if pod.Name != "nginx" {
		t.Errorf("c1 returned name=%q, want nginx", pod.Name)
	}

	// Same key in c2 should return a different object (no cross-cluster collision)
	c2Store := mci.ForCluster("c2").GetStore()
	obj2, exists2, err2 := c2Store.GetByKey("default/nginx")
	if err2 != nil || !exists2 {
		t.Fatalf("c2 GetByKey(default/nginx): exists=%v err=%v", exists2, err2)
	}
	pod2 := obj2.(*simplePod)
	if pod2.Name != "nginx" {
		t.Errorf("c2 returned name=%q, want nginx", pod2.Name)
	}

	// Non-existent key
	_, exists3, err3 := c1Store.GetByKey("default/missing")
	if err3 != nil {
		t.Fatalf("GetByKey(missing): err=%v", err3)
	}
	if exists3 {
		t.Error("GetByKey(missing) returned exists=true")
	}
}

func TestForCluster_Store_ListKeys(t *testing.T) {
	mci := New(Config{})

	mci.store.Add(wrap("nginx", "default", "c1"))
	mci.store.Add(wrap("redis", "default", "c1"))
	mci.store.Add(wrap("coredns", "kube-system", "c2"))

	keys := mci.ForCluster("c1").GetStore().ListKeys()
	if len(keys) != 2 {
		t.Errorf("c1 ListKeys() = %v, want 2 keys", keys)
	}

	// Keys should be in namespace/name format
	keySet := map[string]bool{}
	for _, k := range keys {
		keySet[k] = true
	}
	if !keySet["default/nginx"] || !keySet["default/redis"] {
		t.Errorf("c1 ListKeys() = %v, want [default/nginx, default/redis]", keys)
	}
}

func TestForCluster_Store_ReadOnly(t *testing.T) {
	mci := New(Config{})
	store := mci.ForCluster("c1").GetStore()

	if err := store.Add(nil); err == nil {
		t.Error("Add() should return error on read-only store")
	}
	if err := store.Update(nil); err == nil {
		t.Error("Update() should return error on read-only store")
	}
	if err := store.Delete(nil); err == nil {
		t.Error("Delete() should return error on read-only store")
	}
	if err := store.Replace(nil, ""); err == nil {
		t.Error("Replace() should return error on read-only store")
	}
}

func TestForCluster_Indexer_StubMethods(t *testing.T) {
	mci := New(Config{})
	idx := mci.ForCluster("c1").GetIndexer()

	if _, err := idx.Index("", nil); err == nil {
		t.Error("Index() should return error")
	}
	if _, err := idx.IndexKeys("", ""); err == nil {
		t.Error("IndexKeys() should return error")
	}
	if _, err := idx.ByIndex("", ""); err == nil {
		t.Error("ByIndex() should return error")
	}
	if err := idx.AddIndexers(nil); err == nil {
		t.Error("AddIndexers() should return error")
	}
	if vals := idx.ListIndexFuncValues(""); vals != nil {
		t.Errorf("ListIndexFuncValues() = %v, want nil", vals)
	}
	if indexers := idx.GetIndexers(); len(indexers) != 0 {
		t.Errorf("GetIndexers() = %v, want empty", indexers)
	}
}

func TestForCluster_IsStopped(t *testing.T) {
	mci := New(Config{})
	c1 := mci.ForCluster("c1")

	if c1.IsStopped() {
		t.Error("IsStopped() = true before stop")
	}

	mci.mu.Lock()
	mci.stopped = true
	mci.mu.Unlock()

	if !c1.IsStopped() {
		t.Error("IsStopped() = false after parent stopped")
	}
}
