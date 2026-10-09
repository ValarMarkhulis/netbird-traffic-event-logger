package transformer

func FilterByProtocol(events []Event) (ingressEvents, egressEvents []Event) {
	for _, e := range events {
		if e.Protocol == "TCP" || e.Protocol == "UDP" || e.Protocol == "ICMP" || e.Protocol == "ICMPv6" {
			if e.Direction == "ingress" {
				ingressEvents = append(ingressEvents, e)
			} else if e.Direction == "egress" {
				egressEvents = append(egressEvents, e)
			}
		}
	}
	return
}
