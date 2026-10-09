package transformer

import "log/slog"

func isICMP(protocol string) bool {
	return protocol == "ICMP" || protocol == "ICMPv6"
}

// GroupByPeers correlates egress events (sender's perspective) with ingress
// events (router's perspective) to determine the routing peer, then groups
// egress events by (senderPeer, routingPeer, serviceKey). Egress events are
// used as the accumulation unit because they carry authoritative byte data
// from the sender. Ingress events are used only for routing-peer identification.
func GroupByPeers(ingressEvents, egressEvents []Event) map[[3]string][]Event {
	grouped := make(map[[3]string][]Event)
	for _, egress := range egressEvents {
		senderPeer := egress.PublicKey
		var routingPeer, serviceKey string

		if isICMP(egress.Protocol) {
			// Correlate ICMP/ICMPv6 by matching source_ip, dest_ip, icmp_type, icmp_code
			for _, ingress := range ingressEvents {
				if ingress.Protocol == egress.Protocol &&
					ingress.SourceIP == egress.SourceIP &&
					ingress.DestIP == egress.DestIP &&
					ingress.IcmpType == egress.IcmpType &&
					ingress.IcmpCode == egress.IcmpCode {
					routingPeer = ingress.PublicKey
					break
				}
			}
			serviceKey = egress.Protocol
		} else {
			// Correlate TCP/UDP by matching source_port and dest_port
			srcPort := egress.ConnSourcePort
			dstPort := egress.ConnDestPort
			for _, ingress := range ingressEvents {
				if ingress.ConnSourcePort == srcPort && ingress.ConnDestPort == dstPort {
					routingPeer = ingress.PublicKey
					break
				}
			}
			serviceKey = egress.ConnDestPort + "/" + egress.Protocol
		}

		if routingPeer == "" {
			slog.Debug("GroupByPeers: no matching ingress event, dropping egress event",
				"protocol", egress.Protocol, "flow_id", egress.FlowID)
			continue
		}
		key := [3]string{senderPeer, routingPeer, serviceKey}
		grouped[key] = append(grouped[key], egress)
	}
	return grouped
}
