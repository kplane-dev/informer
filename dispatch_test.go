package informer

import (
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/storage"
)

func TestDispatch_AllClustersHandler(t *testing.T) {
	synced := true
	r := newHandlerRegistry(func() bool { return synced })

	var adds []*mcstorage.ObjectWithClusterIdentity
	r.addAllClusters(MultiClusterEventHandlerFuncs{
		AddFunc: func(obj *mcstorage.ObjectWithClusterIdentity, _ bool) {
			adds = append(adds, obj)
		},
	})

	c1 := wrap("nginx", "default", "c1")
	c2 := wrap("coredns", "kube-system", "c2")
	r.dispatchAdd(c1, false)
	r.dispatchAdd(c2, false)

	if len(adds) != 2 {
		t.Errorf("all-clusters handler received %d adds, want 2", len(adds))
	}
	if adds[0].ClusterID != "c1" || adds[1].ClusterID != "c2" {
		t.Errorf("unexpected cluster IDs: %q, %q", adds[0].ClusterID, adds[1].ClusterID)
	}
}

func TestDispatch_PerClusterHandler(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var c1Adds, c2Adds []string
	r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*simplePod)
			c1Adds = append(c1Adds, pod.Name)
		},
	})
	r.addForCluster("c2", cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*simplePod)
			c2Adds = append(c2Adds, pod.Name)
		},
	})

	r.dispatchAdd(wrap("nginx", "default", "c1"), false)
	r.dispatchAdd(wrap("redis", "default", "c1"), false)
	r.dispatchAdd(wrap("coredns", "kube-system", "c2"), false)

	if len(c1Adds) != 2 {
		t.Errorf("c1 handler received %d adds, want 2", len(c1Adds))
	}
	if len(c2Adds) != 1 {
		t.Errorf("c2 handler received %d adds, want 1", len(c2Adds))
	}
}

func TestDispatch_PerClusterHandler_ReceivesUnwrapped(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var received interface{}
	r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { received = obj },
	})

	r.dispatchAdd(wrap("nginx", "default", "c1"), false)

	if _, ok := received.(*mcstorage.ObjectWithClusterIdentity); ok {
		t.Error("per-cluster handler received wrapped object, want unwrapped")
	}
	if pod, ok := received.(*simplePod); !ok || pod.Name != "nginx" {
		t.Errorf("received = %T %v, want *simplePod nginx", received, received)
	}
}

func TestDispatch_Update_SameCluster(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var updates int
	r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			updates++
		},
	})

	old := wrap("nginx", "default", "c1")
	new := wrap("nginx", "default", "c1")
	r.dispatchUpdate(old, new)

	if updates != 1 {
		t.Errorf("updates = %d, want 1", updates)
	}
}

func TestDispatch_Update_ClusterTransition(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var c1Deletes, c2Adds int
	r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) { c1Deletes++ },
	})
	r.addForCluster("c2", cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { c2Adds++ },
	})

	old := wrap("nginx", "default", "c1")
	new := wrap("nginx", "default", "c2")
	r.dispatchUpdate(old, new)

	if c1Deletes != 1 {
		t.Errorf("c1 deletes = %d, want 1", c1Deletes)
	}
	if c2Adds != 1 {
		t.Errorf("c2 adds = %d, want 1", c2Adds)
	}
}

func TestDispatch_Delete(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var allDeletes int
	var c1Deletes int
	r.addAllClusters(MultiClusterEventHandlerFuncs{
		DeleteFunc: func(obj *mcstorage.ObjectWithClusterIdentity) { allDeletes++ },
	})
	r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
		DeleteFunc: func(obj interface{}) { c1Deletes++ },
	})

	r.dispatchDelete(wrap("nginx", "default", "c1"))

	if allDeletes != 1 {
		t.Errorf("all-clusters deletes = %d, want 1", allDeletes)
	}
	if c1Deletes != 1 {
		t.Errorf("c1 deletes = %d, want 1", c1Deletes)
	}
}

func TestDispatch_RemoveHandler(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var count int
	reg := r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) { count++ },
	})

	r.dispatchAdd(wrap("nginx", "default", "c1"), false)
	if count != 1 {
		t.Fatalf("count = %d before remove, want 1", count)
	}

	reg.Remove()
	r.dispatchAdd(wrap("redis", "default", "c1"), false)
	if count != 1 {
		t.Errorf("count = %d after remove, want 1 (handler should be removed)", count)
	}
}

func TestDispatch_HandlerRegistration_HasSynced(t *testing.T) {
	synced := false
	r := newHandlerRegistry(func() bool { return synced })

	reg := r.addForCluster("c1", cache.ResourceEventHandlerFuncs{})

	if reg.HasSynced() {
		t.Error("HasSynced() = true before parent synced")
	}

	synced = true
	if reg.HasSynced() {
		t.Error("HasSynced() = true before handler marked synced")
	}

	r.markAllSynced()
	if !reg.HasSynced() {
		t.Error("HasSynced() = false after parent synced and handler marked")
	}
}

func TestDispatch_LateRegistration_PerCluster_HasSynced(t *testing.T) {
	synced := false
	r := newHandlerRegistry(func() bool { return synced })

	// Register before sync — should NOT be synced yet
	early := r.addForCluster("c1", cache.ResourceEventHandlerFuncs{})
	if early.HasSynced() {
		t.Error("early handler: HasSynced() = true before sync")
	}

	// Simulate initial sync completing
	synced = true
	r.markAllSynced()
	if !early.HasSynced() {
		t.Error("early handler: HasSynced() = false after markAllSynced")
	}

	// Register after sync — should be synced immediately
	late := r.addForCluster("c2", cache.ResourceEventHandlerFuncs{})
	if !late.HasSynced() {
		t.Error("late handler: HasSynced() = false, want true (parent already synced)")
	}
}

func TestDispatch_LateRegistration_AllClusters_HasSynced(t *testing.T) {
	synced := false
	r := newHandlerRegistry(func() bool { return synced })

	early := r.addAllClusters(MultiClusterEventHandlerFuncs{})
	if early.HasSynced() {
		t.Error("early handler: HasSynced() = true before sync")
	}

	synced = true
	r.markAllSynced()
	if !early.HasSynced() {
		t.Error("early handler: HasSynced() = false after markAllSynced")
	}

	late := r.addAllClusters(MultiClusterEventHandlerFuncs{})
	if !late.HasSynced() {
		t.Error("late handler: HasSynced() = false, want true (parent already synced)")
	}
}

func TestDispatch_ConcurrentSafety(t *testing.T) {
	r := newHandlerRegistry(func() bool { return true })

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			reg := r.addForCluster("c1", cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {},
			})
			r.dispatchAdd(wrap("pod", "default", "c1"), false)
			reg.Remove()
		}(i)
	}
	wg.Wait()
}

// wrap is defined in store_test.go — reuse it here via package-level function
func wrapForDispatch(name, namespace, clusterID string) *mcstorage.ObjectWithClusterIdentity {
	return &mcstorage.ObjectWithClusterIdentity{
		Object: &simplePod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		},
		ClusterID:  clusterID,
		StorageKey: "/pods/clusters/" + clusterID + "/" + namespace + "/" + name,
	}
}
