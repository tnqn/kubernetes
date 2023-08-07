/*
Copyright 2023 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package conntrack

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
	utilexec "k8s.io/utils/exec"
)

type ipPort struct {
	ip   string
	port int
}

// CleanStaleEntries takes care of flushing stale conntrack entries for services and endpoints.
func CleanStaleEntries(isIPv6 bool, exec utilexec.Interface, svcPortMap proxy.ServicePortMap,
	serviceUpdateResult proxy.UpdateServiceMapResult, endpointUpdateResult proxy.UpdateEndpointMapResult) {

	deleteStaleServiceConntrackEntries(isIPv6, exec, svcPortMap, serviceUpdateResult, endpointUpdateResult)
	deleteStaleEndpointConntrackEntries(exec, svcPortMap, endpointUpdateResult)
}

// deleteStaleServiceConntrackEntries takes care of flushing stale conntrack entries related
// to UDP Service IPs. When a service has no endpoints and we drop traffic to it, conntrack
// may create "black hole" entries for that IP+port. When the service gets endpoints we
// need to delete those entries so further traffic doesn't get dropped.
func deleteStaleServiceConntrackEntries(isIPv6 bool, exec utilexec.Interface, svcPortMap proxy.ServicePortMap, serviceUpdateResult proxy.UpdateServiceMapResult, endpointUpdateResult proxy.UpdateEndpointMapResult) {
	conntrackCleanupServiceIPPorts := []ipPort{}
	conntrackCleanupServiceNodePorts := sets.New[int]()

	getStaleIPPorts := func(svcInfo proxy.ServicePort) {
		conntrackCleanupServiceIPPorts = append(conntrackCleanupServiceIPPorts, ipPort{
			ip:   svcInfo.ClusterIP().String(),
			port: svcInfo.Port(),
		})
		for _, extIP := range svcInfo.ExternalIPStrings() {
			conntrackCleanupServiceIPPorts = append(conntrackCleanupServiceIPPorts, ipPort{
				ip:   extIP,
				port: svcInfo.Port(),
			})
		}
		for _, lbIP := range svcInfo.LoadBalancerVIPStrings() {
			conntrackCleanupServiceIPPorts = append(conntrackCleanupServiceIPPorts, ipPort{
				ip:   lbIP,
				port: svcInfo.Port(),
			})
		}
		nodePort := svcInfo.NodePort()
		if svcInfo.Protocol() == v1.ProtocolUDP && nodePort != 0 {
			conntrackCleanupServiceNodePorts.Insert(nodePort)
		}
	}

	for _, svcInfo := range serviceUpdateResult.DeletedUDPServicePorts {
		getStaleIPPorts(svcInfo)
	}
	// merge newly active services gathered from updateEndpointsMap
	// a UDP service that changes from 0 to non-0 endpoints is newly active.
	for _, svcPortName := range endpointUpdateResult.NewlyActiveUDPServices {
		if svcInfo, ok := svcPortMap[svcPortName]; ok {
			klog.V(4).InfoS("Newly-active UDP service may have stale conntrack entries", "servicePortName", svcPortName)
			getStaleIPPorts(svcInfo)
		}
	}

	klog.V(4).InfoS("Deleting conntrack stale entries for services", "IPPorts", conntrackCleanupServiceIPPorts)
	for _, ipPort := range conntrackCleanupServiceIPPorts {
		if err := ClearEntriesForIPPort(exec, ipPort.ip, ipPort.port, v1.ProtocolUDP); err != nil {
			klog.ErrorS(err, "Failed to delete stale service connections", "IP", ipPort.ip, "port", ipPort.port)
		}
	}
	klog.V(4).InfoS("Deleting conntrack stale entries for services", "nodePorts", conntrackCleanupServiceNodePorts.UnsortedList())
	for _, nodePort := range conntrackCleanupServiceNodePorts.UnsortedList() {
		err := ClearEntriesForPort(exec, nodePort, isIPv6, v1.ProtocolUDP)
		if err != nil {
			klog.ErrorS(err, "Failed to clear udp conntrack", "nodePort", nodePort)
		}
	}
}

// deleteStaleEndpointConntrackEntries takes care of flushing stale conntrack entries related
// to UDP endpoints. After a UDP endpoint is removed we must flush any conntrack entries
// for it so that if the same client keeps sending, the packets will get routed to a new endpoint.
func deleteStaleEndpointConntrackEntries(exec utilexec.Interface, svcPortMap proxy.ServicePortMap, endpointUpdateResult proxy.UpdateEndpointMapResult) {
	for _, epSvcPair := range endpointUpdateResult.DeletedUDPEndpoints {
		if svcInfo, ok := svcPortMap[epSvcPair.ServicePortName]; ok {
			endpointIP := proxyutil.IPPart(epSvcPair.Endpoint)
			nodePort := svcInfo.NodePort()
			var err error
			if nodePort != 0 {
				err = ClearEntriesForPortNAT(exec, endpointIP, nodePort, v1.ProtocolUDP)
				if err != nil {
					klog.ErrorS(err, "Failed to delete nodeport-related endpoint connections", "servicePortName", epSvcPair.ServicePortName)
				}
			}
			err = ClearEntriesForNAT(exec, svcInfo.ClusterIP().String(), endpointIP, v1.ProtocolUDP)
			if err != nil {
				klog.ErrorS(err, "Failed to delete endpoint connections", "servicePortName", epSvcPair.ServicePortName)
			}
			for _, extIP := range svcInfo.ExternalIPStrings() {
				err := ClearEntriesForNAT(exec, extIP, endpointIP, v1.ProtocolUDP)
				if err != nil {
					klog.ErrorS(err, "Failed to delete endpoint connections for externalIP", "servicePortName", epSvcPair.ServicePortName, "externalIP", extIP)
				}
			}
			for _, lbIP := range svcInfo.LoadBalancerVIPStrings() {
				err := ClearEntriesForNAT(exec, lbIP, endpointIP, v1.ProtocolUDP)
				if err != nil {
					klog.ErrorS(err, "Failed to delete endpoint connections for LoadBalancerIP", "servicePortName", epSvcPair.ServicePortName, "loadBalancerIP", lbIP)
				}
			}
		}
	}
}
