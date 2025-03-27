package fscrypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"io"
)

func Decrypt(r io.Reader, pass []byte) (io.Reader, error) {
	key := md5.Sum(pass)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	iv := generateIVFromPass(pass)
	stream := cipher.NewCTR(block, iv)

	buf := new(bytes.Buffer)
	writer := &cipher.StreamWriter{S: stream, W: buf}

	if _, err := io.Copy(writer, r); err != nil {
		return nil, err
	}

	return buf, nil
}
