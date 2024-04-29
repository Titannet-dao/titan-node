package locator

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/Filecoin-Titan/titan/api"
	"github.com/Filecoin-Titan/titan/api/types"
	"github.com/Filecoin-Titan/titan/node/config"
	"github.com/Filecoin-Titan/titan/node/handler"
	"github.com/Filecoin-Titan/titan/region"
	"github.com/ipfs/go-cid"
	"github.com/miekg/dns"
)

const (
	prefixOfCandidateNodeID = "c_"
	lengthOfNodeIDV1        = 32
	lengthOfNodeIDV2        = 36
	maxTXTRecord            = 10000
	txtRecordExpireTime     = 30 * time.Minute
)

type TXTRecord struct {
	time  time.Time
	value string
}

type DNSServer struct {
	*config.LocatorCfg
	api.Locator
	reg        region.Region
	txtRecords *SafeMap
}

func NewDNSServer(config *config.LocatorCfg, locator api.Locator, reg region.Region) *DNSServer {
	dnsServer := &DNSServer{config, locator, reg, NewSafeMap()}
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

func (ds *DNSServer) deleteExpireTXTRecord() {
	toBeDeleteDomain := make([]string, 0)
	if ds.txtRecords.Len() >= maxTXTRecord {
		ds.txtRecords.Range(func(key, value interface{}) {
			// can not delete key in this range
			domain := key.(string)
			record := value.(TXTRecord)
			if record.time.Add(txtRecordExpireTime).Before(time.Now()) {
				toBeDeleteDomain = append(toBeDeleteDomain, domain)
			}
		})
	}

	if len(toBeDeleteDomain) > 0 {
		for _, domain := range toBeDeleteDomain {
			ds.txtRecords.Delete(domain)
		}
	}
}
func (ds *DNSServer) SetTXTRecord(domain string, value string) error {
	// release record
	ds.deleteExpireTXTRecord()

	if ds.txtRecords.Len() >= maxTXTRecord {
		return fmt.Errorf("current TXTRecord %d, out of max txt record", ds.txtRecords.Len())
	}

	domain = strings.TrimSuffix(domain, ".")
	ds.txtRecords.Set(domain, &TXTRecord{time: time.Now(), value: value})

	return nil
}

func (ds *DNSServer) DeleteTXTRecord(domain string) error {
	ds.txtRecords.Delete(domain)
	return nil
}

type dnsHandler struct {
	dnsServer *DNSServer
}

func (h *dnsHandler) ServeDNS(w dns.ResponseWriter, r *dns.Msg) {
	m := new(dns.Msg)
	m.SetReply(r)
	m.Authoritative = true

	switch r.Opcode {
	case dns.OpcodeQuery:
		h.HandlerQuery(m, w.RemoteAddr().String())
	}

	if err := w.WriteMsg(m); err != nil {
		log.Errorf("dns server write message error %s", err.Error())
	}
}

func (h *dnsHandler) HandlerQuery(m *dns.Msg, remoteAddr string) {
	log.Debugf("HandlerQuery request %#v", *m)
	for _, q := range m.Question {
		domain := strings.TrimSuffix(strings.ToLower(q.Name), ".")
		switch q.Qtype {
		case dns.TypeTXT:
			if err := h.handlerTXTRecord(m, domain); err != nil {
				log.Errorf("handlerTXTRecord error %s", err.Error())
			}
		case dns.TypeCAA:
			if err := h.handlerCAARecord(m, domain); err != nil {
				log.Errorf("handlerCAARecord error %s", err.Error())
			}
		case dns.TypeA:
			log.Debugf("Query for %s, remote address %s\n", q.Name, remoteAddr)

			if ok, err := h.handlerLocalRecord(m, domain); err != nil {
				log.Infof("handlerLocalRecord %s", err.Error())
				return
			} else if ok {
				return
			}

			if ok, err := h.handlerNodeLocation(m, domain); err != nil {
				log.Infof("handlerCandidateLocation %s", err.Error())
				return
			} else if ok {
				return
			}

			if ok, err := h.handlerAssetLocation(m, domain, remoteAddr); err != nil {
				log.Infof("handlerAssetLocation %s", err.Error())
				return
			} else if ok {
				return
			}
		}
	}
}

func (h *dnsHandler) handlerTXTRecord(m *dns.Msg, domain string) error {
	value, ok := h.dnsServer.txtRecords.Get(domain)
	if ok {
		record := value.(*TXTRecord)

		txt := &dns.TXT{}
		txt.Hdr = dns.RR_Header{Name: m.Question[0].Name, Rrtype: dns.TypeTXT, Class: dns.ClassINET, Ttl: 60}
		txt.Txt = []string{record.value}

		m.Answer = append(m.Answer, txt)
		return nil
	}
	return fmt.Errorf("can not find %s txt record", domain)
}

func (h *dnsHandler) handlerCAARecord(m *dns.Msg, domain string) error {
	rr, err := dns.NewRR(fmt.Sprintf("%s CAA 0 issue letsencrypt.org", domain))
	if err == nil {
		m.Answer = append(m.Answer, rr)
	}
	return err
}

func (h *dnsHandler) handlerLocalRecord(m *dns.Msg, domain string) (bool, error) {
	ip, ok := h.dnsServer.DNSRecords[domain]
	if !ok {
		return false, nil
	}

	rr, err := dns.NewRR(fmt.Sprintf("%s A %s", domain, ip))
	if err == nil {
		m.Answer = append(m.Answer, rr)
	}
	return true, err
}

func (h *dnsHandler) handlerNodeLocation(m *dns.Msg, domain string) (bool, error) {
	fields := strings.Split(domain, ".")
	if len(fields) < 4 {
		return false, fmt.Errorf("invalid domain %s", domain)
	}

	nodeID := fields[0]
	// check node id
	if !h.isMatchNodeID(nodeID) {
		return false, nil
	}

	// add prefix
	nodeID = fmt.Sprintf("%s%s", prefixOfCandidateNodeID, fields[0])
	ip, err := h.dnsServer.GetCandidateIP(context.Background(), nodeID)
	if err != nil {
		return false, err
	}

	if rr, err := dns.NewRR(fmt.Sprintf("%s A %s", domain, ip)); err == nil {
		m.Answer = append(m.Answer, rr)
	}
	return true, err
}

func (h *dnsHandler) handlerAssetLocation(m *dns.Msg, domain, remoteAddr string) (bool, error) {
	fields := strings.Split(domain, ".")
	if len(fields) < 4 {
		return false, fmt.Errorf("invalid domain %s", domain)
	}
	// find candidate for asset
	cid := fields[0]
	if !h.isValidCID(cid) {
		return false, fmt.Errorf("invalid cid %s", cid)
	}

	ctx := context.WithValue(context.Background(), handler.RemoteAddr{}, remoteAddr)
	infos, err := h.dnsServer.CandidateDownloadInfos(ctx, cid)
	if err != nil {
		return false, err
	}

	userIP, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		return false, err
	}

	ipList := h.getIPList(infos)
	if len(ipList) == 0 {
		return false, fmt.Errorf("can not get candidate for cid %s", cid)
	}

	targetIP := getUserNearestIP(userIP, ipList, h.dnsServer.reg)
	rr := &dns.A{
		Hdr: dns.RR_Header{
			Name:   domain,
			Rrtype: dns.TypeA,
			Class:  dns.ClassINET,
			Ttl:    60, // 60s
		},
		A: net.ParseIP(targetIP),
	}
	m.Answer = append(m.Answer, rr)

	return true, nil
}

func (h *dnsHandler) getIPList(downloadInfos []*types.CandidateDownloadInfo) []string {
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

func (h *dnsHandler) isMatchNodeID(id string) bool {
	id = strings.TrimSpace(id)
	return len(id) == lengthOfNodeIDV1 || len(id) == lengthOfNodeIDV2
}

func (h *dnsHandler) isValidCID(cidString string) bool {
	_, err := cid.Decode(cidString)
	return err == nil
}
