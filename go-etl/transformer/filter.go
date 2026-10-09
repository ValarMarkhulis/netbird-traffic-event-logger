package transformer

// FilterByProtocol splits events into ingress and egress, keeping only
// protocols the Group stage can correlate. SCTP, GRE and other protocols are
// dropped here and never become sessions; see "Protocols outside TCP / UDP /
// ICMP" in go-etl/README.md.
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
