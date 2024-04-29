OUT_PATH=./buildshared/windows
TARGET=gol2.dll

VERSION='-X github.com/Filecoin-Titan/titan/build.CurrentCommit=+windows'
LDFLAGS='-extldflags -Wl,-soname,'"$TARGET $VERSION"''

GOOS=windows GOARCH=amd64 CGO_ENABLED=1 go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/x86_64/$TARGET ./cmd/titan-edge

