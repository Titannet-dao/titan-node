OUT_PATH=./buildshared/mac
LIB_NAME=gol2

COMMIT_HASH=$(git rev-parse HEAD | cut -c 1-7)
VERSION='-X github.com/Filecoin-Titan/titan/build.CurrentCommit=+git.'"$COMMIT_HASH"'+mac'

CGO_ENABLED=1 GOOS=darwin GOARCH=arm64 SDK=macos go build -ldflags ''"$VERSION-$(go env GOARCH)"'' -trimpath -buildmode=c-shared -o $OUT_PATH/${LIB_NAME}_arm64.dylib ./cmd/titan-edge
CGO_ENABLED=1 GOOS=darwin GOARCH=amd64 SDK=macos go build -ldflags ''"$VERSION-$(go env GOARCH)"'' -trimpath -buildmode=c-shared -o $OUT_PATH/${LIB_NAME}_amd64.dylib ./cmd/titan-edge
lipo -create $OUT_PATH/${LIB_NAME}_arm64.dylib $OUT_PATH/${LIB_NAME}_amd64.dylib -output $OUT_PATH/${LIB_NAME}.dylib
install_name_tool -id "@rpath/${LIB_NAME}.dylib" $OUT_PATH/${LIB_NAME}.dylib
rm  $OUT_PATH/${LIB_NAME}_*.h
rm $OUT_PATH/${LIB_NAME}_*.dylib