package transformer

import (
	"fmt"
	"sort"

	"github.com/ValarMarkhulis/traffic-event-logger/go-etl/data"
)

func Load(sessions []data.Session) {
	sessionID := 1
	for _, sess := range sessions {
		flowIDs := []string{}
		for f := range sess.FlowIDs {
			flowIDs = append(flowIDs, f)
		}
		sort.Strings(flowIDs)
		srcPorts := []string{}
		for p := range sess.SrcPorts {
			srcPorts = append(srcPorts, p)
		}
		sort.Strings(srcPorts)
		srcIPs := []string{}
		for ip := range sess.SrcIPs {
			srcIPs = append(srcIPs, ip)
		}
		sort.Strings(srcIPs)
		destIPs := []string{}
		for ip := range sess.DestIPs {
			destIPs = append(destIPs, ip)
		}
		sort.Strings(destIPs)
		if isICMP(sess.DestPort) {
			fmt.Printf("Session %d: %s sent %s via %s as routing peer.\n", sessionID, sess.SenderPeer, sess.DestPort, sess.RoutingPeer)
		} else {
			fmt.Printf("Session %d: %s connected to port %s via %s as routing peer.\n", sessionID, sess.SenderPeer, sess.DestPort, sess.RoutingPeer)
		}
		sessionID++
		fmt.Printf("  Flow IDs: %s\n", Join(flowIDs))
		fmt.Printf("  Source ports used: %s\n", Join(srcPorts))
		fmt.Printf("  Source IPs: %s\n", Join(srcIPs))
		fmt.Printf("  Destination IPs: %s\n", Join(destIPs))
		if sess.Start != nil {
			fmt.Printf("  Start: %s\n", sess.Start.Format("2006-01-02 15:04:05.000000"))
		} else {
			fmt.Printf("  Start: <none>\n")
		}
		if sess.End != nil {
			fmt.Printf("  End:   %s\n", sess.End.Format("2006-01-02 15:04:05.000000"))
		} else {
			fmt.Printf("  End:   <none>\n")
		}
		fmt.Printf("  RX Packets: %d\n", sess.RxPackets)
		fmt.Printf("  TX Packets: %d\n", sess.TxPackets)
		fmt.Printf("  RX Bytes:   %d\n", sess.RxBytes)
		fmt.Printf("  TX Bytes:   %d\n", sess.TxBytes)
		fmt.Println()
	}
}
