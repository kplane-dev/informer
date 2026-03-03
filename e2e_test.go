package informer

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	apistorage "k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/storage/etcd3"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/apimachinery/pkg/api/apitesting"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/clock"

	mcstorage "github.com/kplane-dev/storage"
)

var (
	e2eScheme = runtime.NewScheme()
	e2eCodecs = serializer.NewCodecFactory(e2eScheme)
)

func init() {
	metav1.AddToGroupVersion(e2eScheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(e2eScheme))
	utilruntime.Must(examplev1.AddToScheme(e2eScheme))
}

func newPod() runtime.Object     { return &example.Pod{} }
func newPodList() runtime.Object { return &example.PodList{} }

func getPodAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod, ok := obj.(*example.Pod)
	if !ok {
		return nil, nil, fmt.Errorf("not a pod")
	}
	fs := fields.Set{
		"metadata.name":      pod.Name,
		"metadata.namespace": pod.Namespace,
		"spec.nodeName":      pod.Spec.NodeName,
	}
	return labels.Set(pod.Labels), fs, nil
}

func e2eKeyFunc(prefix string) func(runtime.Object) (string, error) {
	return func(obj runtime.Object) (string, error) {
		pod, ok := obj.(*example.Pod)
		if !ok {
			return "", fmt.Errorf("not a pod")
		}
		cluster := pod.Labels["cluster"]
		if cluster == "" {
			cluster = "default"
		}
		return fmt.Sprintf("%s%s/%s/%s", prefix, cluster, pod.Namespace, pod.Name), nil
	}
}

func makePod(name, namespace, cluster string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels:    map[string]string{"cluster": cluster},
		},
	}
}

// setupE2E creates an etcd-backed cacher with cluster identity hooks.
// Returns the delegator (for the informer), raw storage (for writes), and cleanup.
func setupE2E(t *testing.T) (context.Context, apistorage.Interface, apistorage.Interface, func()) {
	t.Helper()

	server, _ := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	codec := apitesting.TestCodec(e2eCodecs, examplev1.SchemeGroupVersion)
	compactor := etcd3.NewCompactor(server.V3Client.Client, 0, clock.RealClock{}, nil)
	t.Cleanup(compactor.Stop)

	prefix := "/pods/clusters/"
	rawStorage, err := etcd3.New(
		server.V3Client,
		compactor,
		codec,
		newPod,
		newPodList,
		etcd3testing.PathPrefix(),
		prefix,
		schema.GroupResource{Resource: "pods"},
		identity.NewEncryptCheckTransformer(),
		etcd3.NewDefaultLeaseManagerConfig(),
		etcd3.NewDefaultDecoder(codec, apistorage.APIObjectVersioner{}),
		apistorage.APIObjectVersioner{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(rawStorage.Close)

	listErrors := 1
	if clientfeatures.FeatureGates().Enabled(clientfeatures.WatchListClient) {
		listErrors = 0
	}
	wrappedStorage := &storagetesting.StorageInjectingListErrors{
		Interface: rawStorage,
		Errors:    listErrors,
	}

	kl := mcstorage.DefaultKeyLayout()

	cacherConfig := cacherstorage.Config{
		Storage:             wrappedStorage,
		Versioner:           apistorage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "pods"},
		EventsHistoryWindow: cacherstorage.DefaultEventFreshDuration,
		ResourcePrefix:      prefix,
		KeyFunc:             e2eKeyFunc(prefix),
		GetAttrsFunc:        getPodAttrs,
		NewFunc:             newPod,
		NewListFunc:         newPodList,
		Codec:               codec,
		IdentityFromKey:     kl.IdentityFromKey(),
		WrapWatchObject: func(object runtime.Object, key string, clusterID string) runtime.Object {
			return &mcstorage.ObjectWithClusterIdentity{
				Object:     object,
				ClusterID:  clusterID,
				StorageKey: key,
			}
		},
	}

	cacher, err := cacherstorage.NewCacherFromConfig(cacherConfig)
	if err != nil {
		t.Fatalf("Failed to create cacher: %v", err)
	}
	ctx := context.Background()

	if err := wait.PollInfinite(100*time.Millisecond, wrappedStorage.ErrorsConsumed); err != nil {
		t.Fatalf("Failed to inject list errors: %v", err)
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.ResilientWatchCacheInitialization) {
		if err := cacher.Wait(ctx); err != nil {
			t.Fatal(err)
		}
	}

	delegator := cacherstorage.NewCacheDelegator(cacher, wrappedStorage)

	terminate := func() {
		delegator.Stop()
		cacher.Stop()
		server.Terminate(t)
	}

	return ctx, delegator, rawStorage, terminate
}

// TestE2E_InitialSync verifies that objects created before the informer starts
// appear in the store with correct cluster IDs after initial sync.
func TestE2E_InitialSync(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	// Create pods in two clusters before starting the informer
	pods := []struct{ name, ns, cluster string }{
		{"nginx", "default", "c1"},
		{"redis", "default", "c1"},
		{"coredns", "kube-system", "c2"},
	}
	for _, p := range pods {
		pod := makePod(p.name, p.ns, p.cluster)
		key := fmt.Sprintf("/pods/clusters/%s/%s/%s", p.cluster, p.ns, p.name)
		if err := rawStorage.Create(ctx, key, pod, &example.Pod{}, 0); err != nil {
			t.Fatalf("Failed to create %s: %v", p.name, err)
		}
	}

	// Create and start the MultiClusterInformer
	mci := New(Config{
		Storage:        delegator,
		ResourcePrefix: "/pods/clusters/",
		GroupResource:  schema.GroupResource{Resource: "pods"},
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go mci.Run(ctx)

	// Wait for initial sync
	if !waitFor(t, 10*time.Second, mci.HasSynced) {
		t.Fatal("timed out waiting for initial sync")
	}

	// Verify store contents
	if mci.Store().Len() != 3 {
		t.Errorf("store has %d objects, want 3", mci.Store().Len())
	}

	c1Objects := mci.List("c1")
	if len(c1Objects) != 2 {
		t.Errorf("List(c1) = %d objects, want 2", len(c1Objects))
	}

	c2Objects := mci.List("c2")
	if len(c2Objects) != 1 {
		t.Errorf("List(c2) = %d objects, want 1", len(c2Objects))
	}

	// Verify Get
	obj, found := mci.Get("c1", "default", "nginx")
	if !found {
		t.Error("Get(c1, default, nginx) not found")
	} else {
		pod := obj.(*example.Pod)
		if pod.Name != "nginx" {
			t.Errorf("Get returned name=%q, want nginx", pod.Name)
		}
	}

	// Verify Clusters
	clusters := mci.Clusters()
	if len(clusters) != 2 {
		t.Errorf("Clusters() = %v, want 2 clusters", clusters)
	}
}

// TestE2E_LiveEvents verifies that create/update/delete events after initial sync
// dispatch to the correct per-cluster handlers.
func TestE2E_LiveEvents(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	mci := New(Config{
		Storage:        delegator,
		ResourcePrefix: "/pods/clusters/",
		GroupResource:  schema.GroupResource{Resource: "pods"},
	})

	var mu sync.Mutex
	var c1Adds, c2Adds []string
	var c1Updates, c1Deletes int

	c1View := mci.ForCluster("c1")
	c1View.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*example.Pod)
			mu.Lock()
			c1Adds = append(c1Adds, pod.Name)
			mu.Unlock()
		},
		UpdateFunc: func(_, _ interface{}) {
			mu.Lock()
			c1Updates++
			mu.Unlock()
		},
		DeleteFunc: func(_ interface{}) {
			mu.Lock()
			c1Deletes++
			mu.Unlock()
		},
	})

	c2View := mci.ForCluster("c2")
	c2View.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*example.Pod)
			mu.Lock()
			c2Adds = append(c2Adds, pod.Name)
			mu.Unlock()
		},
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go mci.Run(ctx)

	if !waitFor(t, 10*time.Second, mci.HasSynced) {
		t.Fatal("timed out waiting for initial sync")
	}

	// Create a pod in c1
	pod := makePod("nginx", "default", "c1")
	if err := rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", pod, &example.Pod{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Create a pod in c2
	pod2 := makePod("coredns", "kube-system", "c2")
	if err := rawStorage.Create(ctx, "/pods/clusters/c2/kube-system/coredns", pod2, &example.Pod{}, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// Wait for adds
	if !waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(c1Adds) >= 1 && len(c2Adds) >= 1
	}) {
		t.Fatal("timed out waiting for add events")
	}

	// Update the c1 pod
	updated := &example.Pod{}
	if err := rawStorage.GuaranteedUpdate(ctx, "/pods/clusters/c1/default/nginx", updated, false, nil,
		func(input runtime.Object, _ apistorage.ResponseMeta) (runtime.Object, *uint64, error) {
			p := input.(*example.Pod)
			if p.Labels == nil {
				p.Labels = make(map[string]string)
			}
			p.Labels["updated"] = "true"
			return p, nil, nil
		}, nil); err != nil {
		t.Fatalf("Update: %v", err)
	}

	if !waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return c1Updates >= 1
	}) {
		t.Fatal("timed out waiting for update event")
	}

	// Delete the c1 pod
	if err := rawStorage.Delete(ctx, "/pods/clusters/c1/default/nginx", &example.Pod{}, nil,
		apistorage.ValidateAllObjectFunc, nil, apistorage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if !waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return c1Deletes >= 1
	}) {
		t.Fatal("timed out waiting for delete event")
	}

	// Verify per-cluster isolation
	mu.Lock()
	if len(c1Adds) != 1 || c1Adds[0] != "nginx" {
		t.Errorf("c1 adds = %v, want [nginx]", c1Adds)
	}
	if len(c2Adds) != 1 || c2Adds[0] != "coredns" {
		t.Errorf("c2 adds = %v, want [coredns]", c2Adds)
	}
	mu.Unlock()
}

// TestE2E_ForCluster_StoreIsolation verifies that ForCluster().GetStore()
// only returns objects for the target cluster.
func TestE2E_ForCluster_StoreIsolation(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	// Create pods in two clusters
	rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", makePod("nginx", "default", "c1"), &example.Pod{}, 0)
	rawStorage.Create(ctx, "/pods/clusters/c1/default/redis", makePod("redis", "default", "c1"), &example.Pod{}, 0)
	rawStorage.Create(ctx, "/pods/clusters/c2/default/nginx", makePod("nginx", "default", "c2"), &example.Pod{}, 0)

	mci := New(Config{
		Storage:        delegator,
		ResourcePrefix: "/pods/clusters/",
		GroupResource:  schema.GroupResource{Resource: "pods"},
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go mci.Run(ctx)

	if !waitFor(t, 10*time.Second, mci.HasSynced) {
		t.Fatal("timed out waiting for initial sync")
	}

	c1Store := mci.ForCluster("c1").GetStore()
	c1Items := c1Store.List()
	if len(c1Items) != 2 {
		t.Errorf("c1 store has %d items, want 2", len(c1Items))
	}

	c2Store := mci.ForCluster("c2").GetStore()
	c2Items := c2Store.List()
	if len(c2Items) != 1 {
		t.Errorf("c2 store has %d items, want 1", len(c2Items))
	}

	// Verify GetByKey works with namespace/name format
	obj, exists, err := c1Store.GetByKey("default/nginx")
	if err != nil || !exists {
		t.Fatalf("c1 GetByKey(default/nginx): exists=%v err=%v", exists, err)
	}
	pod := obj.(*example.Pod)
	if pod.Name != "nginx" {
		t.Errorf("GetByKey returned name=%q, want nginx", pod.Name)
	}

	// Same name in c2 should return a different object
	obj2, exists2, _ := c2Store.GetByKey("default/nginx")
	if !exists2 {
		t.Fatal("c2 GetByKey(default/nginx) not found")
	}
	pod2 := obj2.(*example.Pod)
	if pod2.Name != "nginx" {
		t.Errorf("c2 GetByKey returned name=%q", pod2.Name)
	}

	// Verify objects are unwrapped
	for _, item := range c1Items {
		if _, ok := item.(*mcstorage.ObjectWithClusterIdentity); ok {
			t.Error("per-cluster store returned wrapped object")
		}
	}
}

// TestE2E_AllClustersHandler verifies that the all-clusters handler receives
// wrapped events with cluster identity.
func TestE2E_AllClustersHandler(t *testing.T) {
	ctx, delegator, rawStorage, terminate := setupE2E(t)
	defer terminate()

	mci := New(Config{
		Storage:        delegator,
		ResourcePrefix: "/pods/clusters/",
		GroupResource:  schema.GroupResource{Resource: "pods"},
	})

	var mu sync.Mutex
	events := make(map[string]string) // name → clusterID

	mci.AddEventHandler(MultiClusterEventHandlerFuncs{
		AddFunc: func(obj *mcstorage.ObjectWithClusterIdentity, _ bool) {
			mu.Lock()
			events[obj.GetName()] = obj.ClusterID
			mu.Unlock()
		},
	})

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go mci.Run(ctx)

	if !waitFor(t, 10*time.Second, mci.HasSynced) {
		t.Fatal("timed out waiting for initial sync")
	}

	rawStorage.Create(ctx, "/pods/clusters/c1/default/nginx", makePod("nginx", "default", "c1"), &example.Pod{}, 0)
	rawStorage.Create(ctx, "/pods/clusters/c2/default/redis", makePod("redis", "default", "c2"), &example.Pod{}, 0)

	if !waitFor(t, 10*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 2
	}) {
		t.Fatal("timed out waiting for events")
	}

	mu.Lock()
	defer mu.Unlock()
	if events["nginx"] != "c1" {
		t.Errorf("nginx cluster = %q, want c1", events["nginx"])
	}
	if events["redis"] != "c2" {
		t.Errorf("redis cluster = %q, want c2", events["redis"])
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		if condition() {
			return true
		}
		select {
		case <-deadline:
			return false
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
}
