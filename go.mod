module github.com/Filecoin-Titan/titan

go 1.17

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.1
	github.com/BurntSushi/toml v1.2.1
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/docker/go-units v0.5.0
	github.com/fatih/color v1.13.0
	github.com/filecoin-project/go-jsonrpc v0.2.3
	github.com/filecoin-project/go-statemachine v1.0.3
	github.com/filecoin-project/pubsub v1.0.0
	github.com/gabriel-vasile/mimetype v1.4.2
	github.com/gbrlsnchs/jwt/v3 v3.0.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/google/uuid v1.3.0
	github.com/gorilla/mux v1.8.0
	github.com/icza/backscanner v0.0.0-20210726202459-ac2ffc679f94
	github.com/ipfs/go-block-format v0.1.1 // indirect
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-datastore v0.6.0
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-ds-measure v0.2.0
	github.com/ipfs/go-fetcher v1.6.1
	github.com/ipfs/go-fs-lock v0.0.7
	github.com/ipfs/go-ipfs-http-client v0.5.0
	github.com/ipfs/go-ipld-format v0.4.0
	github.com/ipfs/go-ipld-legacy v0.1.1
	github.com/ipfs/go-libipfs v0.7.0
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipfs/go-merkledag v0.10.0
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-unixfsnode v1.6.0
	github.com/ipfs/interface-go-ipfs-core v0.11.1
	github.com/ipld/go-codec-dagpb v1.6.0
	github.com/ipld/go-ipld-prime v0.20.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/lib/pq v1.10.7
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-base32 v0.1.0
	github.com/multiformats/go-multiaddr v0.9.0
	github.com/multiformats/go-multihash v0.2.1
	github.com/oschwald/geoip2-golang v1.7.0
	github.com/prometheus/client_golang v1.14.0
	github.com/quic-go/quic-go v0.33.0
	github.com/shirou/gopsutil/v3 v3.22.9
	github.com/syndtr/goleveldb v1.0.1-0.20210819022825-2ae1ddf74ef7
	github.com/urfave/cli/v2 v2.25.3
	github.com/whyrusleeping/cbor-gen v0.0.0-20230126041949-52956bd4c9aa
	go.opencensus.io v0.24.0
	go.uber.org/fx v1.19.2
	golang.org/x/sys v0.8.0
	golang.org/x/time v0.0.0-20220922220347-f3bd1da661af
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2
)

require (
	github.com/alecthomas/units v0.0.0-20211218093645-b94a6e3cc137
	github.com/ethereum/go-ethereum v1.11.6
	github.com/filecoin-project/go-address v1.1.0
	github.com/filecoin-project/go-fil-commcid v0.1.0
	github.com/filecoin-project/go-fil-commp-hashhash v0.2.0
	github.com/filecoin-project/go-fil-markets v1.28.1
	github.com/filecoin-project/go-state-types v0.11.1
	github.com/ipfs/go-filestore v1.2.1
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-offline v0.3.0
	github.com/ipld/go-car v0.5.0
	github.com/ipld/go-car/v2 v2.8.2
	github.com/miekg/dns v1.1.53
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20190321100706-95778dfbb74e // indirect
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.2 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20170627025303-887ab5e44cc3 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.2.0 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-task/slim-sprig v0.0.0-20230315185526-52ccab3ef572 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/mock v1.6.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.5-0.20220116011046-fa5810519dcb // indirect
	github.com/google/pprof v0.0.0-20230405160723-4a4c7d95572b // indirect
	github.com/gorilla/websocket v1.5.0 // indirect
	github.com/hashicorp/golang-lru v0.6.0
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/go-ipfs-blockstore v1.3.0
	github.com/ipfs/go-ipfs-cmds v0.8.2 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.0 // indirect
	github.com/ipfs/go-ipfs-exchange-interface v0.2.0 // indirect
	github.com/ipfs/go-ipfs-files v0.3.0
	github.com/ipfs/go-ipfs-util v0.0.2 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-path v0.3.1
	github.com/ipfs/go-unixfs v0.4.4
	github.com/ipfs/go-verifcid v0.0.2 // indirect
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magefile/mage v1.9.0 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.18 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.4 // indirect
	github.com/minio/sha256-simd v1.0.1-0.20230130105256-d9c3aea9e949 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multicodec v0.8.1
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/onsi/ginkgo/v2 v2.9.2 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/oschwald/maxminddb-golang v1.9.0 // indirect
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/polydawn/refmt v0.89.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_model v0.3.0 // indirect
	github.com/prometheus/common v0.42.0 // indirect
	github.com/prometheus/procfs v0.9.0 // indirect
	github.com/prometheus/statsd_exporter v0.21.0 // indirect
	github.com/quic-go/qpack v0.4.0 // indirect
	github.com/quic-go/qtls-go1-19 v0.3.2 // indirect
	github.com/quic-go/qtls-go1-20 v0.2.2 // indirect
	github.com/rs/cors v1.7.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/whyrusleeping/cbor v0.0.0-20171005072247-63513f603b11 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opentelemetry.io/otel v1.13.0 // indirect
	go.opentelemetry.io/otel/trace v1.13.0 // indirect
	go.uber.org/dig v1.16.1 // indirect
	go.uber.org/zap v1.24.0 // indirect
	go4.org v0.0.0-20200411211856-f5505b9728dd // indirect
	golang.org/x/crypto v0.9.0
	golang.org/x/exp v0.0.0-20230321023759-10a507213a29 // indirect
	golang.org/x/mod v0.10.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	google.golang.org/genproto v0.0.0-20210917145530-b395a37504d4 // indirect
	google.golang.org/grpc v1.45.0 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

require (
	github.com/GeertJohan/go.incremental v1.0.0 // indirect
	github.com/GeertJohan/go.rice v1.0.3 // indirect
	github.com/StackExchange/wmi v1.2.1 // indirect
	github.com/akavel/rsrc v0.8.0 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/btcsuite/btcd/btcec/v2 v2.3.2 // indirect
	github.com/daaku/go.zipexe v1.0.2 // indirect
	github.com/deckarep/golang-set/v2 v2.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v2 v2.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-amt-ipld/v4 v4.0.0 // indirect
	github.com/filecoin-project/go-bitfield v0.2.4 // indirect
	github.com/filecoin-project/go-cbor-util v0.0.1 // indirect
	github.com/filecoin-project/go-crypto v0.0.1 // indirect
	github.com/filecoin-project/go-data-transfer/v2 v2.0.0-rc6 // indirect
	github.com/filecoin-project/go-hamt-ipld v0.1.5 // indirect
	github.com/filecoin-project/go-hamt-ipld/v2 v2.0.0 // indirect
	github.com/filecoin-project/go-hamt-ipld/v3 v3.1.0 // indirect
	github.com/filecoin-project/go-padreader v0.0.1 // indirect
	github.com/filecoin-project/go-statestore v0.2.0 // indirect
	github.com/filecoin-project/specs-actors v0.9.15 // indirect
	github.com/filecoin-project/specs-actors/v2 v2.3.6 // indirect
	github.com/filecoin-project/specs-actors/v3 v3.1.2 // indirect
	github.com/filecoin-project/specs-actors/v4 v4.0.2 // indirect
	github.com/filecoin-project/specs-actors/v5 v5.0.6 // indirect
	github.com/filecoin-project/specs-actors/v6 v6.0.2 // indirect
	github.com/filecoin-project/specs-actors/v7 v7.0.1 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/hannahhoward/cbor-gen-for v0.0.0-20230214144701-5d17c9d5243c // indirect
	github.com/hannahhoward/go-pubsub v0.0.0-20200423002714-8d62886cc36e // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.2 // indirect
	github.com/holiman/uint256 v1.2.2 // indirect
	github.com/ipfs/go-bitfield v1.1.0 // indirect
	github.com/ipfs/go-graphsync v0.14.5 // indirect
	github.com/ipfs/go-ipfs-posinfo v0.0.1 // indirect
	github.com/ipfs/go-ipld-cbor v0.0.6 // indirect
	github.com/ipsn/go-secp256k1 v0.0.0-20180726113642-9d62b9f0bc52 // indirect
	github.com/jessevdk/go-flags v1.4.0 // indirect
	github.com/jonboulle/clockwork v0.2.2 // indirect
	github.com/jpillora/backoff v1.0.0 // indirect
	github.com/libp2p/go-flow-metrics v0.1.0 // indirect
	github.com/libp2p/go-libp2p v0.27.1 // indirect
	github.com/libp2p/go-libp2p-pubsub v0.9.3 // indirect
	github.com/libp2p/go-msgio v0.3.0 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/multiformats/go-multiaddr-dns v0.3.1 // indirect
	github.com/multiformats/go-multistream v0.4.1 // indirect
	github.com/nkovacs/streamquote v1.0.0 // indirect
	github.com/raulk/clock v1.1.0 // indirect
	github.com/shirou/gopsutil v3.21.4-0.20210419000835-c7a38de76ee5+incompatible // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasttemplate v1.0.1 // indirect
	github.com/whyrusleeping/bencher v0.0.0-20190829221104-bb6607aa8bba // indirect
	github.com/whyrusleeping/chunker v0.0.0-20181014151217-fe64bd25879f // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	gopkg.in/natefinch/npipe.v2 v2.0.0-20160621034901-c1b8fa8bdcce // indirect
	lukechampine.com/blake3 v1.1.7 // indirect
)

require (
	github.com/ipfs/go-blockservice v0.5.0
	github.com/klauspost/cpuid/v2 v2.2.4 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	go.etcd.io/etcd v0.0.0-20191023171146-3cf2f69b5738
	golang.org/x/tools v0.7.0 // indirect
)

replace github.com/filecoin-project/go-jsonrpc v0.2.3 => github.com/zscboy/go-jsonrpc v0.2.3

replace google.golang.org/grpc => google.golang.org/grpc v1.26.0

replace github.com/coreos/bbolt v1.3.4 => go.etcd.io/bbolt v1.3.4
