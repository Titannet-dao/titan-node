OUT_PATH=./buildshared/linux
TARGET=libgol2.so

VERSION='-X github.com/Filecoin-Titan/titan/build.CurrentCommit=+linux'
LDFLAGS='-extldflags -Wl,-soname,'"$TARGET"''

GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/x86_64/$TARGET ./cmd/titan-edge

