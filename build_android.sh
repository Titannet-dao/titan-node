NDK_PATH=/home/abc/android-ndk-r26c

CC_PATH=$NDK_PATH/toolchains/llvm/prebuilt/linux-x86_64/bin
OUT_PATH=./android-jni/titan-go-so

TARGET=libgol2.so
LDFLAGS='-extldflags -Wl,-soname,'"$TARGET"''
VERSION='-X github.com/Filecoin-Titan/titan/build.CurrentCommit=+android'

GOOS=android GOARCH=amd64 CGO_ENABLED=1 CC=$CC_PATH/x86_64-linux-android34-clang go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/x86_64/$TARGET ./cmd/titan-edge

GOOS=android GOARCH=386 CGO_ENABLED=1 CC=$CC_PATH/i686-linux-android34-clang go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/x86/$TARGET ./cmd/titan-edge

GOOS=android GOARCH=arm64 CGO_ENABLED=1 CC=$CC_PATH/aarch64-linux-android34-clang go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/arm64-v8a/$TARGET ./cmd/titan-edge

GOOS=android GOARCH=arm CGO_ENABLED=1 CC=$CC_PATH/armv7a-linux-androideabi34-clang go build -ldflags ''"$LDFLAGS $VERSION-$(go env GOARCH)"'' -buildmode=c-shared -o $OUT_PATH/armeabi-v7a/$TARGET ./cmd/titan-edge

