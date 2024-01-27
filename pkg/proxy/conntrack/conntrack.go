//go:build linux
// +build linux

/*
Copyright 2016 The Kubernetes Authors.

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
	"fmt"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netlink/nl"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/proxy"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/utils/exec"
	utilnet "k8s.io/utils/net"
)

// Interface for dealing with conntrack
type Interface interface {
	// ClearEntriesForIP deletes conntrack entries for connections of the given
	// protocol, to the given IP.
	ClearEntriesForIP(ip string, protocol v1.Protocol) error

	// ClearEntriesForPort deletes conntrack entries for connections of the given
	// protocol and IP family, to the given port.
	ClearEntriesForPort(port int, isIPv6 bool, protocol v1.Protocol) error

	// ClearEntriesForNAT deletes conntrack entries for connections of the given
	// protocol, which had been DNATted from origin to dest.
	ClearEntriesForNAT(origin, dest string, protocol v1.Protocol) error

	// ClearEntriesForPortNAT deletes conntrack entries for connections of the given
	// protocol, which had been DNATted from the given port (on any IP) to dest.
	ClearEntriesForPortNAT(dest string, port int, protocol v1.Protocol) error

	ClearEntries(servicePorts map[string]proxy.ServicePortName,
		nodePorts map[int]proxy.ServicePortName,
		svcPortNameToEndpointsMap map[proxy.ServicePortName]sets.Set[string],
		isIPv6 bool,
		protocol v1.Protocol) error
}

// execCT implements Interface by execing the conntrack tool
type execCT struct {
	execer exec.Interface
}

func (ct *execCT) ClearEntries(servicePorts map[string]proxy.ServicePortName, nodePorts map[int]proxy.ServicePortName, svcPortNameToEndpointsMap map[proxy.ServicePortName]sets.Set[string], isIPv6 bool, protocol v1.Protocol) error {
	//TODO implement me
	panic("implement me")
}

var _ Interface = &execCT{}

func NewExec(execer exec.Interface) Interface {
	return &execCT{execer: execer}
}

// noConnectionToDelete is the error string returned by conntrack when no matching connections are found
const noConnectionToDelete = "0 flow entries have been deleted"

func protoStr(proto v1.Protocol) string {
	return strings.ToLower(string(proto))
}

func parametersWithFamily(isIPv6 bool, parameters ...string) []string {
	if isIPv6 {
		parameters = append(parameters, "-f", "ipv6")
	}
	return parameters
}

// exec executes the conntrack tool using the given parameters
func (ct *execCT) exec(parameters ...string) error {
	conntrackPath, err := ct.execer.LookPath("conntrack")
	if err != nil {
		return fmt.Errorf("error looking for path of conntrack: %v", err)
	}
	klog.V(4).InfoS("Clearing conntrack entries", "parameters", parameters)
	output, err := ct.execer.Command(conntrackPath, parameters...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("conntrack command returned: %q, error message: %s", string(output), err)
	}
	klog.V(4).InfoS("Conntrack entries deleted", "output", string(output))
	return nil
}

// ClearEntriesForIP is part of Interface
func (ct *execCT) ClearEntriesForIP(ip string, protocol v1.Protocol) error {
	parameters := parametersWithFamily(utilnet.IsIPv6String(ip), "-D", "--orig-dst", ip, "-p", protoStr(protocol))
	err := ct.exec(parameters...)
	if err != nil && !strings.Contains(err.Error(), noConnectionToDelete) {
		// TODO: Better handling for deletion failure. When failure occur, stale udp connection may not get flushed.
		// These stale udp connection will keep black hole traffic. Making this a best effort operation for now, since it
		// is expensive to baby-sit all udp connections to kubernetes services.
		return fmt.Errorf("error deleting connection tracking state for UDP service IP: %s, error: %v", ip, err)
	}
	return nil
}

// ClearEntriesForPort is part of Interface
func (ct *execCT) ClearEntriesForPort(port int, isIPv6 bool, protocol v1.Protocol) error {
	if port <= 0 {
		return fmt.Errorf("wrong port number. The port number must be greater than zero")
	}
	parameters := parametersWithFamily(isIPv6, "-D", "-p", protoStr(protocol), "--dport", strconv.Itoa(port))
	err := ct.exec(parameters...)
	if err != nil && !strings.Contains(err.Error(), noConnectionToDelete) {
		return fmt.Errorf("error deleting conntrack entries for UDP port: %d, error: %v", port, err)
	}
	return nil
}

// ClearEntriesForNAT is part of Interface
func (ct *execCT) ClearEntriesForNAT(origin, dest string, protocol v1.Protocol) error {
	parameters := parametersWithFamily(utilnet.IsIPv6String(origin), "-D", "--orig-dst", origin, "--dst-nat", dest,
		"-p", protoStr(protocol))
	err := ct.exec(parameters...)
	if err != nil && !strings.Contains(err.Error(), noConnectionToDelete) {
		// TODO: Better handling for deletion failure. When failure occur, stale udp connection may not get flushed.
		// These stale udp connection will keep black hole traffic. Making this a best effort operation for now, since it
		// is expensive to baby sit all udp connections to kubernetes services.
		return fmt.Errorf("error deleting conntrack entries for UDP peer {%s, %s}, error: %v", origin, dest, err)
	}
	return nil
}

// ClearEntriesForPortNAT is part of Interface
func (ct *execCT) ClearEntriesForPortNAT(dest string, port int, protocol v1.Protocol) error {
	if port <= 0 {
		return fmt.Errorf("wrong port number. The port number must be greater than zero")
	}
	parameters := parametersWithFamily(utilnet.IsIPv6String(dest), "-D", "-p", protoStr(protocol), "--dport", strconv.Itoa(port), "--dst-nat", dest)
	err := ct.exec(parameters...)
	if err != nil && !strings.Contains(err.Error(), noConnectionToDelete) {
		return fmt.Errorf("error deleting conntrack entries for UDP port: %d, error: %v", port, err)
	}
	return nil
}

type netlinkCT struct {
	netlinkHandle *netlink.Handle
}

var _ Interface = &netlinkCT{}

func NewNetlink() Interface {
	return &netlinkCT{}
}

func (n *netlinkCT) ClearEntriesForIP(ip string, protocol v1.Protocol) error {
	parsedIP := utilnet.ParseIPSloppy(ip)
	filter := &customFilter{matchFn: func(flow *netlink.ConntrackFlow) bool {
		if nl.L4ProtoMap[flow.Forward.Protocol] != protoStr(protocol) {
			return false
		}
		if flow.Forward.DstIP.Equal(parsedIP) {
			return false
		}
		return true
	}}
	matched, err := n.netlinkHandle.ConntrackDeleteFilter(netlink.ConntrackTable, getInetFamily(utilnet.IsIPv6(parsedIP)), filter)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Conntrack entries deleted", "matched", matched)
	return nil
}

func (n *netlinkCT) ClearEntriesForPort(port int, isIPv6 bool, protocol v1.Protocol) error {
	filter := &customFilter{matchFn: func(flow *netlink.ConntrackFlow) bool {
		if nl.L4ProtoMap[flow.Forward.Protocol] != protoStr(protocol) {
			return false
		}
		if int(flow.Forward.DstPort) != port {
			return false
		}
		return true
	}}
	matched, err := n.netlinkHandle.ConntrackDeleteFilter(netlink.ConntrackTable, getInetFamily(isIPv6), filter)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Conntrack entries deleted", "matched", matched)
	return nil
}

func (n *netlinkCT) ClearEntriesForNAT(origin, dest string, protocol v1.Protocol) error {
	originIP := utilnet.ParseIPSloppy(origin)
	destIP := utilnet.ParseIPSloppy(dest)
	filter := &customFilter{matchFn: func(flow *netlink.ConntrackFlow) bool {
		if nl.L4ProtoMap[flow.Forward.Protocol] != protoStr(protocol) {
			return false
		}
		if flow.Forward.DstIP.Equal(originIP) || flow.Reverse.SrcIP.Equal(destIP) {
			return false
		}
		return true
	}}
	matched, err := n.netlinkHandle.ConntrackDeleteFilter(netlink.ConntrackTable, getInetFamily(utilnet.IsIPv6(originIP)), filter)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Conntrack entries deleted", "matched", matched)
	return nil
}

func (n *netlinkCT) ClearEntriesForPortNAT(dest string, port int, protocol v1.Protocol) error {
	destIP := utilnet.ParseIPSloppy(dest)
	filter := &customFilter{matchFn: func(flow *netlink.ConntrackFlow) bool {
		return nl.L4ProtoMap[flow.Forward.Protocol] == protoStr(protocol) &&
			int(flow.Forward.DstPort) == port &&
			flow.Reverse.SrcIP.Equal(destIP)
	}}
	matched, err := n.netlinkHandle.ConntrackDeleteFilter(netlink.ConntrackTable, getInetFamily(utilnet.IsIPv6(destIP)), filter)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Conntrack entries deleted", "matched", matched)
	return nil
}

func (n *netlinkCT) ClearEntries(servicePorts map[string]proxy.ServicePortName,
	nodePorts map[int]proxy.ServicePortName,
	svcPortNameToEndpointsMap map[proxy.ServicePortName]sets.Set[string],
	isIPv6 bool,
	protocol v1.Protocol) error {
	filter := &customFilter{matchFn: func(flow *netlink.ConntrackFlow) bool {
		if nl.L4ProtoMap[flow.Forward.Protocol] != protoStr(protocol) {
			return false
		}
		destIPPort := fmt.Sprintf("%s:%d", flow.Forward.DstIP, flow.Forward.DstPort)
		svcPortName, exists := servicePorts[destIPPort]
		if !exists {
			svcPortName, exists = nodePorts[int(flow.Forward.DstPort)]
			if !exists {
				return false
			}
		}
		endpoints, exists := svcPortNameToEndpointsMap[svcPortName]
		if !exists {
			return true
		}
		natIPPort := fmt.Sprintf("%s:%d", flow.Reverse.SrcIP, flow.Reverse.SrcPort)
		_, exists = endpoints[natIPPort]
		if exists {
			return false
		}
		return true
	}}
	matched, err := n.netlinkHandle.ConntrackDeleteFilter(netlink.ConntrackTable, getInetFamily(isIPv6), filter)
	if err != nil {
		return err
	}
	klog.V(4).InfoS("Conntrack entries deleted", "matched", matched)
	return nil
}

func getInetFamily(isIPv6 bool) netlink.InetFamily {
	if isIPv6 {
		return nl.FAMILY_V6
	} else {
		return nl.FAMILY_V4
	}
}

type customFilter struct {
	matchFn func(flow *netlink.ConntrackFlow) bool
}

func (c *customFilter) MatchConntrackFlow(flow *netlink.ConntrackFlow) bool {
	return c.matchFn(flow)
}
