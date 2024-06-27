OUT_PATH=./buildshared/windows
TARGET=gol2.dll

COMMIT_HASH=$(git rev-parse HEAD | cut -c 1-7)
VERSION='-X github.com/Filecoin-Titan/titan/build.CurrentCommit=+git.'"$COMMIT_HASH"'+windows'
LDFLAGS='-extldflags -Wl,-soname,'"$TARGET $VERSION"''

export CGO_LDFLAGS="-L/d/filecoin-titan -lgoworkerd"
GOOS=windows GOARCH=amd64 CGO_ENABLED=1 go build --tags=edge -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/x86_64/$TARGET ./cmd/titan-edge
