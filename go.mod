module github.com/Filecoin-Titan/titan

go 1.22.0

toolchain go1.23.3

require (
	contrib.go.opencensus.io/exporter/prometheus v0.4.2
	github.com/BurntSushi/toml v1.4.0
	github.com/acarl005/stripansi v0.0.0-20180116102854-5a71ef0e047d
	github.com/docker/go-units v0.5.0
	github.com/fatih/color v1.15.0
	github.com/filecoin-project/go-jsonrpc v0.7.0
	github.com/filecoin-project/go-statemachine v1.0.3
	github.com/filecoin-project/pubsub v1.0.0
	github.com/gabriel-vasile/mimetype v1.4.6
	github.com/gbrlsnchs/jwt/v3 v3.0.1
	github.com/go-sql-driver/mysql v1.6.0
	github.com/google/uuid v1.6.0
	github.com/gorilla/mux v1.8.1
	github.com/icza/backscanner v0.0.0-20230330133933-bf6beb754c70
	github.com/ipfs/go-cid v0.4.1
	github.com/ipfs/go-datastore v0.6.0
	github.com/ipfs/go-ds-leveldb v0.5.0
	github.com/ipfs/go-ds-measure v0.2.0
	github.com/ipfs/go-fetcher v1.6.1
	github.com/ipfs/go-fs-lock v0.0.7
	github.com/ipfs/go-ipld-format v0.6.0
	github.com/ipfs/go-ipld-legacy v0.2.1
	github.com/ipfs/go-libipfs v0.7.0
	github.com/ipfs/go-log/v2 v2.5.1
	github.com/ipfs/go-merkledag v0.11.0
	github.com/ipfs/go-metrics-interface v0.0.1
	github.com/ipfs/go-unixfsnode v1.9.2
	github.com/ipfs/interface-go-ipfs-core v0.11.1
	github.com/ipld/go-codec-dagpb v1.6.0
	github.com/ipld/go-ipld-prime v0.21.0
	github.com/jmoiron/sqlx v1.3.5
	github.com/kelseyhightower/envconfig v1.4.0
	github.com/lib/pq v1.10.7
	github.com/mitchellh/go-homedir v1.1.0
	github.com/multiformats/go-base32 v0.1.0
	github.com/multiformats/go-multiaddr v0.14.0
	github.com/multiformats/go-multihash v0.2.3
	github.com/oschwald/geoip2-golang v1.11.0
	github.com/prometheus/client_golang v1.20.5
	github.com/quic-go/quic-go v0.48.2
	github.com/shirou/gopsutil/v3 v3.23.1
	github.com/syndtr/goleveldb v1.0.1-0.20220721030215-126854af5e6d
	github.com/urfave/cli/v2 v2.25.7
	github.com/whyrusleeping/cbor-gen v0.2.0
	go.opencensus.io v0.24.0
	go.uber.org/fx v1.23.0
	golang.org/x/sys v0.27.0
	golang.org/x/time v0.8.0
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da
)

require (
	github.com/Masterminds/squirrel v1.5.4
	github.com/alecthomas/units v0.0.0-20240927000941-0f3dac36c52b
	github.com/aws/aws-sdk-go v1.55.5
	github.com/google/go-github/v61 v61.0.0
	github.com/gorilla/websocket v1.5.3
	github.com/ipfs/boxo v0.24.3
	github.com/ipfs/go-block-format v0.2.0
	github.com/ipfs/go-filestore v1.2.1
	github.com/ipfs/go-ipfs-chunker v0.0.5
	github.com/ipfs/go-ipfs-exchange-offline v0.3.0
	github.com/ipld/go-car v0.6.2
	github.com/ipld/go-car/v2 v2.14.2
	github.com/miekg/dns v1.1.62
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1
	github.com/nfnt/resize v0.0.0-20180221191011-83c6a9932646
	github.com/pkg/errors v0.9.1
	github.com/u2takey/ffmpeg-go v0.5.0
	github.com/utopiosphe/titan-storage-sdk v0.0.0-20250108035649-7537a83bec0f
	go.etcd.io/etcd/api/v3 v3.5.12
	go.etcd.io/etcd/client/v3 v3.5.12
	golang.org/x/image v0.21.0
	k8s.io/client-go v0.30.3
)

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/blang/semver/v4 v4.0.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/coreos/go-systemd/v22 v22.5.0 // indirect
	github.com/cpuguy83/go-md2man/v2 v2.0.3 // indirect
	github.com/crackcomm/go-gitignore v0.0.0-20241020182519-7843d2ba8fdf // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dgraph-io/ristretto v0.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/eikenb/pipeat v0.0.0-20210730190139-06b3e6902001 // indirect
	github.com/facebookgo/atomicfile v0.0.0-20151019160806-2de1f203e7d5 // indirect
	github.com/go-kit/log v0.2.1 // indirect
	github.com/go-logfmt/logfmt v0.5.1 // indirect
	github.com/go-logr/logr v1.4.2 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.4 // indirect
	github.com/google/go-querystring v1.1.0 // indirect
	github.com/google/gofuzz v1.2.0 // indirect
	github.com/google/gopacket v1.1.19 // indirect
	github.com/google/pprof v0.0.0-20241128161848-dc51965c6481 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/hashicorp/golang-lru/v2 v2.0.7 // indirect
	github.com/ipfs/bbloom v0.0.4 // indirect
	github.com/ipfs/go-bitfield v1.1.0 // indirect
	github.com/ipfs/go-ipfs-cmds v0.14.0 // indirect
	github.com/ipfs/go-ipfs-ds-help v1.1.1 // indirect
	github.com/ipfs/go-ipfs-exchange-interface v0.2.1 // indirect
	github.com/ipfs/go-ipfs-posinfo v0.0.1 // indirect
	github.com/ipfs/go-ipfs-util v0.0.3 // indirect
	github.com/ipfs/go-ipld-cbor v0.2.0 // indirect
	github.com/ipfs/go-log v1.0.5 // indirect
	github.com/ipfs/go-verifcid v0.0.3 // indirect
	github.com/jbenet/goprocess v0.1.4 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/compress v1.17.11 // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/lann/builder v0.0.0-20180802200727-47ae307949d0 // indirect
	github.com/lann/ps v0.0.0-20150810152359-62de8c46ede0 // indirect
	github.com/libp2p/go-buffer-pool v0.1.0 // indirect
	github.com/libp2p/go-cidranger v1.1.0 // indirect
	github.com/libp2p/go-libp2p-asn-util v0.4.1 // indirect
	github.com/libp2p/go-libp2p-kad-dht v0.28.1 // indirect
	github.com/libp2p/go-libp2p-kbucket v0.6.4 // indirect
	github.com/libp2p/go-libp2p-record v0.2.0 // indirect
	github.com/libp2p/go-libp2p-routing-helpers v0.7.4 // indirect
	github.com/libp2p/go-msgio v0.3.0 // indirect
	github.com/libp2p/go-netroute v0.2.2 // indirect
	github.com/lufia/plan9stats v0.0.0-20230110061619-bbe2e5e100de // indirect
	github.com/magefile/mage v1.9.0 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/minio/sha256-simd v1.0.1 // indirect
	github.com/moby/spdystream v0.2.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/mr-tron/base58 v1.2.0 // indirect
	github.com/multiformats/go-base36 v0.2.0 // indirect
	github.com/multiformats/go-multiaddr-dns v0.4.0 // indirect
	github.com/multiformats/go-multibase v0.2.0 // indirect
	github.com/multiformats/go-multistream v0.6.0 // indirect
	github.com/multiformats/go-varint v0.0.7 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/mxk/go-flowrate v0.0.0-20140419014527-cca7078d478f // indirect
	github.com/onsi/ginkgo/v2 v2.22.0 // indirect
	github.com/opentracing/opentracing-go v1.2.0 // indirect
	github.com/oschwald/maxminddb-golang v1.13.1 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/petar/GoLLRB v0.0.0-20210522233825-ae3b015fd3e9 // indirect
	github.com/philhofer/fwd v1.1.2 // indirect
	github.com/pion/ice/v2 v2.3.37 // indirect
	github.com/pion/sctp v1.8.34 // indirect
	github.com/polydawn/refmt v0.89.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20221212215047-62379fc7944b // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.60.1 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/prometheus/statsd_exporter v0.22.7 // indirect
	github.com/quic-go/qpack v0.5.1 // indirect
	github.com/rs/cors v1.11.1 // indirect
	github.com/rs/xid v1.5.0 // indirect
	github.com/russross/blackfriday/v2 v2.1.0 // indirect
	github.com/samber/lo v1.47.0 // indirect
	github.com/secure-io/sio-go v0.3.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	github.com/stretchr/testify v1.10.0 // indirect
	github.com/tinylib/msgp v1.1.8 // indirect
	github.com/tklauser/go-sysconf v0.3.11 // indirect
	github.com/tklauser/numcpus v0.6.0 // indirect
	github.com/tus/lockfile v1.2.0 // indirect
	github.com/u2takey/go-utils v0.3.1 // indirect
	github.com/whyrusleeping/base32 v0.0.0-20170828182744-c30ac30633cc // indirect
	github.com/whyrusleeping/cbor v0.0.0-20171005072247-63513f603b11 // indirect
	github.com/whyrusleeping/chunker v0.0.0-20181014151217-fe64bd25879f // indirect
	github.com/whyrusleeping/go-keyspace v0.0.0-20160322163242-5b898ac5add1 // indirect
	github.com/xrash/smetrics v0.0.0-20201216005158-039620a65673 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.etcd.io/etcd/client/pkg/v3 v3.5.12 // indirect
	go.opentelemetry.io/otel v1.32.0 // indirect
	go.opentelemetry.io/otel/metric v1.32.0 // indirect
	go.opentelemetry.io/otel/trace v1.32.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
	go.uber.org/dig v1.18.0 // indirect
	go.uber.org/mock v0.5.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	go4.org v0.0.0-20230225012048-214862532bf5 // indirect
	golang.org/x/crypto v0.29.0 // indirect
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f // indirect
	golang.org/x/mod v0.22.0 // indirect
	golang.org/x/net v0.31.0 // indirect
	golang.org/x/oauth2 v0.23.0 // indirect
	golang.org/x/sync v0.9.0 // indirect
	golang.org/x/term v0.26.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	golang.org/x/tools v0.27.0 // indirect
	gonum.org/v1/gonum v0.15.1 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20241007155032-5fefd90f89a9 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20241015192408-796eee8c2d53 // indirect
	google.golang.org/grpc v1.67.1 // indirect
	google.golang.org/protobuf v1.35.2 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	k8s.io/api v0.30.3 // indirect
	k8s.io/apimachinery v0.30.3 // indirect
	k8s.io/klog/v2 v2.120.1 // indirect
	k8s.io/utils v0.0.0-20230726121419-3b25d923346b // indirect
	lukechampine.com/blake3 v1.3.0 // indirect
	sigs.k8s.io/json v0.0.0-20221116044647-bc3834ca7abd // indirect
	sigs.k8s.io/structured-merge-diff/v4 v4.4.1 // indirect
	sigs.k8s.io/yaml v1.3.0 // indirect
)

require (
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0 // indirect
	github.com/golang/snappy v0.0.5-0.20220116011046-fa5810519dcb // indirect
	github.com/hashicorp/golang-lru v1.0.2
	github.com/ipfs/go-ipfs-blockstore v1.3.1
	github.com/ipfs/go-ipfs-files v0.3.0
	github.com/ipfs/go-path v0.3.1
	github.com/ipfs/go-unixfs v0.4.5
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/multiformats/go-multicodec v0.9.0
)

require (
	github.com/filecoin-project/go-cbor-util v0.0.1 // indirect
	github.com/filecoin-project/go-statestore v0.2.0 // indirect
	github.com/golang/geo v0.0.0-20230421003525-6adc56603217
	github.com/libp2p/go-libp2p v0.37.1 // indirect
	github.com/minio/madmin-go/v3 v3.0.9
	github.com/minio/minio-go/v7 v7.0.61
)

require (
	github.com/chai2010/webp v1.1.1
	github.com/ipfs/go-blockservice v0.5.2
	github.com/ipfs/kubo v0.30.0
	github.com/tus/tusd/v2 v2.6.0
)

// replace github.com/utopiosphe/titan-storage-sdk => ../titan-storage-sdk
