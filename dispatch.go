package informer

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/client-go/tools/cache"

	mcstorage "github.com/kplane-dev/storage"
)

// MultiClusterEventHandler receives events with cluster identity intact.
// Objects are *storage.ObjectWithClusterIdentity envelopes.
type MultiClusterEventHandler interface {
	OnAdd(obj *mcstorage.ObjectWithClusterIdentity, isInInitialList bool)
	OnUpdate(oldObj, newObj *mcstorage.ObjectWithClusterIdentity)
	OnDelete(obj *mcstorage.ObjectWithClusterIdentity)
}

// MultiClusterEventHandlerFuncs is a convenience adapter for MultiClusterEventHandler.
type MultiClusterEventHandlerFuncs struct {
	AddFunc    func(obj *mcstorage.ObjectWithClusterIdentity, isInInitialList bool)
	UpdateFunc func(oldObj, newObj *mcstorage.ObjectWithClusterIdentity)
	DeleteFunc func(obj *mcstorage.ObjectWithClusterIdentity)
}

func (f MultiClusterEventHandlerFuncs) OnAdd(obj *mcstorage.ObjectWithClusterIdentity, isInInitialList bool) {
	if f.AddFunc != nil {
		f.AddFunc(obj, isInInitialList)
	}
}

func (f MultiClusterEventHandlerFuncs) OnUpdate(oldObj, newObj *mcstorage.ObjectWithClusterIdentity) {
	if f.UpdateFunc != nil {
		f.UpdateFunc(oldObj, newObj)
	}
}

func (f MultiClusterEventHandlerFuncs) OnDelete(obj *mcstorage.ObjectWithClusterIdentity) {
	if f.DeleteFunc != nil {
		f.DeleteFunc(obj)
	}
}

// HandlerRegistration is returned when a handler is registered. It can be used
// to check sync status or remove the handler.
type HandlerRegistration interface {
	// HasSynced returns true when the parent informer has completed its
	// initial sync and this handler has been marked as synced. Handlers
	// registered before initial sync completes are marked when the
	// initial-events-end bookmark arrives. Handlers registered after sync
	// are marked immediately upon registration.
	HasSynced() bool
	// Remove removes this handler registration.
	Remove() error
}

type handlerRegistry struct {
	mu              sync.RWMutex
	nextID          int
	allHandlers     map[int]*registeredHandler
	clusterHandlers map[string]map[int]*registeredHandler
	parentSynced    func() bool
}

type registeredHandler struct {
	id        int
	clusterID string // empty = all-clusters handler
	handler   interface{}
	synced    atomic.Bool
}

func newHandlerRegistry(parentSynced func() bool) *handlerRegistry {
	return &handlerRegistry{
		allHandlers:     make(map[int]*registeredHandler),
		clusterHandlers: make(map[string]map[int]*registeredHandler),
		parentSynced:    parentSynced,
	}
}

// addAllClusters registers a MultiClusterEventHandler.
// If the informer has already completed initial sync, the handler is marked
// synced immediately — there is no "initial list" to replay for late registrations.
func (r *handlerRegistry) addAllClusters(handler MultiClusterEventHandler) *handlerRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	h := &registeredHandler{id: id, handler: handler}
	if r.parentSynced() {
		h.synced.Store(true)
	}
	r.allHandlers[id] = h
	return &handlerRegistration{registry: r, handler: h}
}

// addForCluster registers a cache.ResourceEventHandler for a specific cluster.
// If the informer has already completed initial sync, the handler is marked
// synced immediately — there is no "initial list" to replay for late registrations.
func (r *handlerRegistry) addForCluster(clusterID string, handler cache.ResourceEventHandler) *handlerRegistration {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := r.nextID
	r.nextID++
	h := &registeredHandler{id: id, clusterID: clusterID, handler: handler}
	if r.parentSynced() {
		h.synced.Store(true)
	}
	if r.clusterHandlers[clusterID] == nil {
		r.clusterHandlers[clusterID] = make(map[int]*registeredHandler)
	}
	r.clusterHandlers[clusterID][id] = h
	return &handlerRegistration{registry: r, handler: h}
}

func (r *handlerRegistry) remove(h *registeredHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if h.clusterID == "" {
		delete(r.allHandlers, h.id)
	} else {
		if m, ok := r.clusterHandlers[h.clusterID]; ok {
			delete(m, h.id)
			if len(m) == 0 {
				delete(r.clusterHandlers, h.clusterID)
			}
		}
	}
}

func (r *handlerRegistry) dispatchAdd(env *mcstorage.ObjectWithClusterIdentity, isInitialList bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, h := range r.allHandlers {
		if mch, ok := h.handler.(MultiClusterEventHandler); ok {
			mch.OnAdd(env, isInitialList)
		}
	}

	if handlers, ok := r.clusterHandlers[env.ClusterID]; ok {
		obj := unwrapCacheableObject(env.Object)
		for _, h := range handlers {
			if reh, ok := h.handler.(cache.ResourceEventHandler); ok {
				reh.OnAdd(obj, isInitialList)
			}
		}
	}
}

func (r *handlerRegistry) dispatchUpdate(oldEnv, newEnv *mcstorage.ObjectWithClusterIdentity) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, h := range r.allHandlers {
		if mch, ok := h.handler.(MultiClusterEventHandler); ok {
			mch.OnUpdate(oldEnv, newEnv)
		}
	}

	oldCluster := oldEnv.ClusterID
	newCluster := newEnv.ClusterID

	if oldCluster == newCluster {
		// Same cluster: dispatch update to per-cluster handlers
		if handlers, ok := r.clusterHandlers[newCluster]; ok {
			oldObj := unwrapCacheableObject(oldEnv.Object)
			newObj := unwrapCacheableObject(newEnv.Object)
			for _, h := range handlers {
				if reh, ok := h.handler.(cache.ResourceEventHandler); ok {
					reh.OnUpdate(oldObj, newObj)
				}
			}
		}
	} else {
		// Cluster changed (rare but defensive): old cluster sees delete, new sees add
		if handlers, ok := r.clusterHandlers[oldCluster]; ok {
			oldObj := unwrapCacheableObject(oldEnv.Object)
			for _, h := range handlers {
				if reh, ok := h.handler.(cache.ResourceEventHandler); ok {
					reh.OnDelete(oldObj)
				}
			}
		}
		if handlers, ok := r.clusterHandlers[newCluster]; ok {
			newObj := unwrapCacheableObject(newEnv.Object)
			for _, h := range handlers {
				if reh, ok := h.handler.(cache.ResourceEventHandler); ok {
					reh.OnAdd(newObj, false)
				}
			}
		}
	}
}

func (r *handlerRegistry) dispatchDelete(env *mcstorage.ObjectWithClusterIdentity) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, h := range r.allHandlers {
		if mch, ok := h.handler.(MultiClusterEventHandler); ok {
			mch.OnDelete(env)
		}
	}

	if handlers, ok := r.clusterHandlers[env.ClusterID]; ok {
		obj := unwrapCacheableObject(env.Object)
		for _, h := range handlers {
			if reh, ok := h.handler.(cache.ResourceEventHandler); ok {
				reh.OnDelete(obj)
			}
		}
	}
}

// markAllSynced marks all currently registered handlers as synced.
func (r *handlerRegistry) markAllSynced() {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, h := range r.allHandlers {
		h.synced.Store(true)
	}
	for _, handlers := range r.clusterHandlers {
		for _, h := range handlers {
			h.synced.Store(true)
		}
	}
}

// handlerRegistration implements HandlerRegistration.
type handlerRegistration struct {
	registry *handlerRegistry
	handler  *registeredHandler
}

func (r *handlerRegistration) HasSynced() bool {
	return r.registry.parentSynced() && r.handler.synced.Load()
}

func (r *handlerRegistration) HasSyncedChecker() cache.DoneChecker {
	return &registrationDoneChecker{reg: r}
}

func (r *handlerRegistration) Remove() error {
	r.registry.remove(r.handler)
	return nil
}

// registrationDoneChecker implements cache.DoneChecker for a handler registration.
type registrationDoneChecker struct {
	reg *handlerRegistration
}

func (d *registrationDoneChecker) Name() string {
	return fmt.Sprintf("handler-%d", d.reg.handler.id)
}

func (d *registrationDoneChecker) Done() <-chan struct{} {
	ch := make(chan struct{})
	go func() {
		for !d.reg.HasSynced() {
			time.Sleep(10 * time.Millisecond)
		}
		close(ch)
	}()
	return ch
}
