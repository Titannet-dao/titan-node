package fscrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"io"
	"os"
	"path"

	"github.com/google/uuid"
)

var chunkSize = 1024 * 1024

func generateIVFromPass(pass []byte) []byte {
	hash := md5.Sum(pass)
	return hash[:aes.BlockSize]
}

func Encrypt(r io.Reader, pass []byte, tempDir string) (io.Reader, int64, error) {

	key := md5.Sum(pass)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, 0, err
	}

	iv := generateIVFromPass(pass)
	stream := cipher.NewCTR(block, iv)

	tempFilePath := path.Join(tempDir, uuid.NewString()+".enc.tmp")
	tmpFile, err := os.Create(tempFilePath)
	if err != nil {
		return nil, 0, err
	}
	defer tmpFile.Close()

	var (
		buf        = make([]byte, chunkSize)
		totalBytes = int64(0)
		writer     = &cipher.StreamWriter{S: stream, W: tmpFile}
	)
	for {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return nil, 0, err
		}
		if n == 0 {
			break
		}

		if _, err = writer.Write(buf[:n]); err != nil {
			return nil, 0, err
		}

		totalBytes += int64(n)
	}

	if err = tmpFile.Sync(); err != nil {
		return nil, 0, err
	}

	t, err := os.Open(tempFilePath)
	if err != nil {
		return nil, 0, err
	}

	return t, totalBytes, nil
}
