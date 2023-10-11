package locator

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/miekg/dns"
)

const (
	prefixOfCandidateNodeID = "c_"
	lengthOfCandidateNodeID = 32
)

type DNSServer struct {
	*config.LocatorCfg
	api.Locator
	reg region.Region
}

func NewDNSServer(config *config.LocatorCfg, locator api.Locator, reg region.Region) *DNSServer {
	dnsServer := &DNSServer{config, locator, reg}
	go dnsServer.start()

	return dnsServer
}

func (ds *DNSServer) start() {
	handler := &dnsHandler{dnsServer: ds}
	server := &dns.Server{
		Addr:      ds.DNSServerAddress,
		Net:       "udp",
		Handler:   handler,
		UDPSize:   65535,
		ReusePort: true,
	}

	log.Debugf("Starting DNS server on %s", ds.DNSServerAddress)
	err := server.ListenAndServe()
	if err != nil {
		log.Errorf("Failed to start dns server: %s\n", err.Error())
	}
}

type dnsHandler struct {
	dnsServer *DNSServer
}

func (h *dnsHandler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Compress = false
	switch r.Opcode {
	case dns.OpcodeQuery:
		h.parseQuery(m, w.RemoteAddr().String())
	}

	if err := w.WriteMsg(m); err != nil {
		log.Errorf("dns server write message error %s", err.Error())
	}
}

func (h *dnsHandler) parseQuery(m *dns.Msg, remoteAddr string) {
	for _, q := range m.Question {
		switch q.Qtype {
		case dns.TypeA:
			log.Debugf("Query for %s, remote address %s\n", q.Name, remoteAddr)
			name := strings.ToLower(q.Name)
			ip, ok := h.dnsServer.DNSRecords[strings.TrimSuffix(name, ".")]
			if ok {
				if rr, err := dns.NewRR(fmt.Sprintf("%s A %s", name, ip)); err == nil {
					m.Answer = append(m.Answer, rr)
				} else {
					log.Errorf("NewRR error %s", err.Error())
				}
				return
			}

			fields := strings.Split(name, ".")
			if len(fields) < 4 {
				log.Errorf("invalid domain %s", name)
				return
			}

			// find candidate by id
			if h.isMatchCandidateID(fields[0]) {
				// add prefix
				nodeID := fmt.Sprintf("%s%s", prefixOfCandidateNodeID, fields[0])
				ip, err := h.dnsServer.GetCandidateIP(context.Background(), nodeID)
				if err != nil {
					log.Errorf("GetCandidateIP error %s", err.Error())
					return
				}

				if rr, err := dns.NewRR(fmt.Sprintf("%s A %s", name, ip)); err == nil {
					m.Answer = append(m.Answer, rr)
				} else {
					log.Errorf("NewRR error %s", err.Error())
				}
				return
			}

			// find candidate for asset
			cid := fields[0]
			ctx := context.WithValue(context.Background(), handler.RemoteAddr{}, remoteAddr)
			infos, err := h.dnsServer.CandidateDownloadInfos(ctx, cid)
			if err != nil {
				log.Errorf("get candidate error %s, cid %s", err.Error(), cid)
				return
			}

			userIP, _, err := net.SplitHostPort(remoteAddr)
			if err != nil {
				log.Errorf("parase dns client addr error %s, remoteAddr %s", err.Error(), remoteAddr)
				return
			}

			ips := h.getIPs(infos)
			if len(ips) == 0 {
				log.Errorf("can not get candidate for cid %s", cid)
				return
			}

			targetIP := getUserNearestIP(userIP, ips, h.dnsServer.reg)
			rr := &dns.A{
				Hdr: dns.RR_Header{
					Name:   name,
					Rrtype: dns.TypeA,
					Class:  dns.ClassINET,
					Ttl:    60, // 60s
				},
				A: net.ParseIP(targetIP),
			}
			m.Answer = append(m.Answer, rr)
		}
	}
}

func (h *dnsHandler) getIPs(downloadInfos []*types.CandidateDownloadInfo) []string {
	if len(downloadInfos) == 0 {
		return []string{}
	}

	ips := make([]string, 0, len(downloadInfos))
	for _, info := range downloadInfos {
		ip, _, err := net.SplitHostPort(info.Address)
		if err != nil {
			log.Errorf("parse candidate address error %s, address %s", err.Error(), info.Address)
			continue
		}
		ips = append(ips, ip)
	}
	return ips
}

func (h *dnsHandler) isMatchCandidateID(id string) bool {
	id = strings.TrimSpace(id)
	return len(id) == lengthOfCandidateNodeID
}
