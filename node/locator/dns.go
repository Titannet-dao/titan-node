package locator

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/miekg/dns"
)

type DNSServer struct {
	*config.LocatorCfg
	api.Locator
}

func NewDNSServer(config *config.LocatorCfg, locator api.Locator) *DNSServer {
	dnsServer := &DNSServer{config, locator}
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
			fields := strings.Split(q.Name, ".")
			if len(fields) < 4 {
				log.Errorf("invalid domain %s", q.Name)
				return
			}

			cid := fields[0]
			ctx := context.WithValue(context.Background(), handler.RemoteAddr{}, remoteAddr)
			infos, err := h.dnsServer.CandidateDownloadInfos(ctx, cid)
			if err != nil {
				log.Errorf("get candidate error %s, cid %s", err.Error(), cid)
				return
			}

			if len(infos) == 0 {
				log.Errorf("can not get candidate for cid %s", cid)
				rr, err := dns.NewRR(fmt.Sprintf("%s A %s", q.Name, "127.0.0.1"))
				if err == nil {
					m.Answer = append(m.Answer, rr)
				}
				return
			}

			ip, _, err := net.SplitHostPort(infos[0].Address)
			if err != nil {
				log.Errorf("parse candidate address error %s, address %s", err.Error(), infos[0].Address)
				return
			}

			rr, err := dns.NewRR(fmt.Sprintf("%s A %s", q.Name, ip))
			if err == nil {
				m.Answer = append(m.Answer, rr)
			}

		}
	}
}
