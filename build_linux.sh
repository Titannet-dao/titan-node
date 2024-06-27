OUT_PATH=./buildshared/linux
TARGET=libgol2.so

COMMIT_HASH=$(git rev-parse HEAD | cut -c 1-7)
VERSION='-X github.com/Filecoin-Titan/titan/build.CurrentCommit=+git.'"$COMMIT_HASH"'+linux'
LDFLAGS='-extldflags -Wl,-soname,'"$TARGET"''

GOOS=linux GOARCH=amd64 CGO_ENABLED=1 go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/x86_64/$TARGET ./cmd/titan-edge

