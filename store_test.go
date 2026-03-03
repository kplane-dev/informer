package informer

import (
	"sort"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/storage"
)

type simplePod struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
}

func (p *simplePod) DeepCopyObject() runtime.Object {
	out := *p
	return &out
}

func wrap(name, namespace, clusterID string) *mcstorage.ObjectWithClusterIdentity {
	return &mcstorage.ObjectWithClusterIdentity{
		Object: &simplePod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		},
		ClusterID:  clusterID,
		StorageKey: "/pods/clusters/" + clusterID + "/" + namespace + "/" + name,
	}
}

func TestCompositeKeyFunc(t *testing.T) {
	tests := []struct {
		name      string
		namespace string
		cluster   string
		wantKey   string
	}{
		{"nginx", "default", "c1", "c1/default/nginx"},
		{"coredns", "kube-system", "c2", "c2/kube-system/coredns"},
		{"node1", "", "c1", "c1/node1"}, // cluster-scoped
	}
	for _, tt := range tests {
		env := wrap(tt.name, tt.namespace, tt.cluster)
		key, err := CompositeKeyFunc(env)
		if err != nil {
			t.Errorf("CompositeKeyFunc(%s/%s/%s) error: %v", tt.cluster, tt.namespace, tt.name, err)
			continue
		}
		if key != tt.wantKey {
			t.Errorf("CompositeKeyFunc(%s/%s/%s) = %q, want %q", tt.cluster, tt.namespace, tt.name, key, tt.wantKey)
		}
	}
}

func TestCompositeKeyFunc_Tombstone(t *testing.T) {
	tombstone := cache.DeletedFinalStateUnknown{
		Key: "c1/default/nginx",
		Obj: wrap("nginx", "default", "c1"),
	}
	key, err := CompositeKeyFunc(tombstone)
	if err != nil {
		t.Fatalf("CompositeKeyFunc(tombstone) error: %v", err)
	}
	if key != "c1/default/nginx" {
		t.Errorf("CompositeKeyFunc(tombstone) = %q, want %q", key, "c1/default/nginx")
	}
}

func TestCompositeKeyFunc_NoCrossClusterCollision(t *testing.T) {
	c1 := wrap("nginx", "default", "c1")
	c2 := wrap("nginx", "default", "c2")

	key1, _ := CompositeKeyFunc(c1)
	key2, _ := CompositeKeyFunc(c2)

	if key1 == key2 {
		t.Errorf("same-named objects in different clusters have same key: %q", key1)
	}
}

func TestClusterIndexFunc(t *testing.T) {
	env := wrap("nginx", "default", "c1")
	keys, err := ClusterIndexFunc(env)
	if err != nil {
		t.Fatalf("ClusterIndexFunc error: %v", err)
	}
	if len(keys) != 1 || keys[0] != "c1" {
		t.Errorf("ClusterIndexFunc = %v, want [c1]", keys)
	}
}

func TestClusterIndexFunc_Tombstone(t *testing.T) {
	tombstone := cache.DeletedFinalStateUnknown{
		Key: "c1/default/nginx",
		Obj: wrap("nginx", "default", "c1"),
	}
	keys, err := ClusterIndexFunc(tombstone)
	if err != nil {
		t.Fatalf("ClusterIndexFunc(tombstone) error: %v", err)
	}
	if len(keys) != 1 || keys[0] != "c1" {
		t.Errorf("ClusterIndexFunc(tombstone) = %v, want [c1]", keys)
	}
}

func TestClusterIndexFunc_UnknownType(t *testing.T) {
	obj := &simplePod{ObjectMeta: metav1.ObjectMeta{Name: "test"}}
	keys, err := ClusterIndexFunc(obj)
	if err != nil {
		t.Fatalf("ClusterIndexFunc(non-wrapped) error: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("ClusterIndexFunc(non-wrapped) = %v, want empty", keys)
	}
}

func TestCompositeStore_AddAndGet(t *testing.T) {
	s := NewCompositeStore()

	c1nginx := wrap("nginx", "default", "c1")
	c2nginx := wrap("nginx", "default", "c2")

	if err := s.Add(c1nginx); err != nil {
		t.Fatalf("Add c1/nginx: %v", err)
	}
	if err := s.Add(c2nginx); err != nil {
		t.Fatalf("Add c2/nginx: %v", err)
	}

	// Both should be stored without collision
	if s.Len() != 2 {
		t.Errorf("Len() = %d, want 2", s.Len())
	}

	got, exists, err := s.Get("c1", "default", "nginx")
	if err != nil || !exists {
		t.Fatalf("Get c1/default/nginx: exists=%v err=%v", exists, err)
	}
	if got.ClusterID != "c1" {
		t.Errorf("got ClusterID = %q, want c1", got.ClusterID)
	}

	got, exists, err = s.Get("c2", "default", "nginx")
	if err != nil || !exists {
		t.Fatalf("Get c2/default/nginx: exists=%v err=%v", exists, err)
	}
	if got.ClusterID != "c2" {
		t.Errorf("got ClusterID = %q, want c2", got.ClusterID)
	}
}

func TestCompositeStore_ListByCluster(t *testing.T) {
	s := NewCompositeStore()
	s.Add(wrap("nginx", "default", "c1"))
	s.Add(wrap("redis", "default", "c1"))
	s.Add(wrap("coredns", "kube-system", "c2"))

	c1Objects := s.ListByCluster("c1")
	if len(c1Objects) != 2 {
		t.Errorf("ListByCluster(c1) = %d objects, want 2", len(c1Objects))
	}

	c2Objects := s.ListByCluster("c2")
	if len(c2Objects) != 1 {
		t.Errorf("ListByCluster(c2) = %d objects, want 1", len(c2Objects))
	}

	// Verify returned objects are unwrapped
	for _, obj := range c1Objects {
		if _, ok := obj.(*mcstorage.ObjectWithClusterIdentity); ok {
			t.Error("ListByCluster returned wrapped object, want unwrapped")
		}
		if obj.GetObjectKind() == nil {
			t.Error("ListByCluster returned nil ObjectKind")
		}
	}
}

func TestCompositeStore_ListAll(t *testing.T) {
	s := NewCompositeStore()
	s.Add(wrap("nginx", "default", "c1"))
	s.Add(wrap("redis", "default", "c1"))
	s.Add(wrap("coredns", "kube-system", "c2"))

	all := s.ListAll()
	if len(all) != 3 {
		t.Errorf("ListAll() = %d objects, want 3", len(all))
	}
}

func TestCompositeStore_Clusters(t *testing.T) {
	s := NewCompositeStore()
	s.Add(wrap("nginx", "default", "c1"))
	s.Add(wrap("redis", "default", "c1"))
	s.Add(wrap("coredns", "kube-system", "c2"))
	s.Add(wrap("etcd", "kube-system", "c3"))

	clusters := s.Clusters()
	sort.Strings(clusters)
	if len(clusters) != 3 || clusters[0] != "c1" || clusters[1] != "c2" || clusters[2] != "c3" {
		t.Errorf("Clusters() = %v, want [c1 c2 c3]", clusters)
	}
}

func TestCompositeStore_Delete(t *testing.T) {
	s := NewCompositeStore()
	env := wrap("nginx", "default", "c1")
	s.Add(env)

	if s.Len() != 1 {
		t.Fatalf("before delete: Len() = %d", s.Len())
	}

	if err := s.Delete(env); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if s.Len() != 0 {
		t.Errorf("after delete: Len() = %d, want 0", s.Len())
	}
}

func TestCompositeStore_Update(t *testing.T) {
	s := NewCompositeStore()
	env := wrap("nginx", "default", "c1")
	s.Add(env)

	// Update with new labels
	updated := &mcstorage.ObjectWithClusterIdentity{
		Object: &simplePod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "nginx",
				Namespace: "default",
				Labels:    map[string]string{"updated": "true"},
			},
		},
		ClusterID: "c1",
	}
	if err := s.Update(updated); err != nil {
		t.Fatalf("Update: %v", err)
	}

	got, exists, _ := s.Get("c1", "default", "nginx")
	if !exists {
		t.Fatal("object not found after update")
	}
	pod := got.Object.(*simplePod)
	if pod.Labels["updated"] != "true" {
		t.Errorf("update did not take effect")
	}
	if s.Len() != 1 {
		t.Errorf("Len() = %d after update, want 1", s.Len())
	}
}

// Verify that simplePod implements runtime.Object (compile check)
var _ runtime.Object = (*simplePod)(nil)

// Verify simplePod's ObjectKind
func init() {
	_ = schema.GroupVersionKind{}
}
