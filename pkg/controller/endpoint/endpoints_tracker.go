package endpoint

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sync"
)

// EndpointsTracker tracks EndpointSlices and their associated generation to
// help determine if a change to an EndpointSlice has been processed by the
// EndpointSlice controller.
type EndpointsTracker struct {
	// lock protects generationsByService.
	lock sync.RWMutex
	// generationsByService tracks the generations of EndpointSlices for each
	// Service.
	resourceVersions map[types.NamespacedName]string
}

func (t *EndpointsTracker) UpdateResourceVersion(endpoint *corev1.Endpoints) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.resourceVersions[types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Name}] = endpoint.ResourceVersion
}

func (t *EndpointsTracker) StaleEndpoints(endpoint *corev1.Endpoints) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	resourceVersion, ok := t.resourceVersions[types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Name}]
	if ok && resourceVersion == endpoint.ResourceVersion {
		return false
	}
	return true
}

func (t *EndpointsTracker) ShouldSync(endpoint *corev1.Endpoints) bool {
	t.lock.RLock()
	defer t.lock.RUnlock()
	resourceVersion, ok := t.resourceVersions[types.NamespacedName{Namespace: endpoint.Namespace, Name: endpoint.Name}]
	if ok && resourceVersion == endpoint.ResourceVersion {
		return false
	}
	return true
}