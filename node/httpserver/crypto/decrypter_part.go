package fscrypto

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"io"
	"os"

	"github.com/ipfs/go-libipfs/files"
)

type DecryptPartReader struct {
	file     *os.File
	stream   cipher.Stream
	fileSize int64
	// out      *os.File
}

func NewDecryptPartIPFSStreamer(reader files.File, pass []byte) (cipher.Stream, error) {
	key := md5.Sum(pass)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	iv := generateIVFromPass(pass)
	stream := cipher.NewCTR(block, iv)

	return stream, nil
}

// NewDecryptPartReader creates a new DecryptPartReader
func NewDecryptPartReader(filePath string, pass []byte) (*DecryptPartReader, error) {
	key := md5.Sum(pass)
	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, err
	}

	iv := generateIVFromPass(pass)
	stream := cipher.NewCTR(block, iv)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	// var out *os.File
	// if outPath != "" {
	// 	out, err = os.Create(outPath)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// }

	return &DecryptPartReader{
		file: file,
		// out:      out,
		stream:   stream,
		fileSize: fileInfo.Size(),
	}, nil
}

func (d *DecryptPartReader) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 || off >= d.fileSize {
		return 0, io.EOF
	}

	if off+int64(len(p)) > d.fileSize {
		p = p[:d.fileSize-off]
	}

	encryptedData := make([]byte, len(p))
	_, err = d.file.ReadAt(encryptedData, off)
	if err != nil && err != io.EOF {
		return 0, err
	}

	// Decrypt the read data
	d.stream.XORKeyStream(p, encryptedData)

	return len(p), nil
}

// // WriteAt implements the io.WriterAt interface
// func (d *DecryptPartReader) WriteAt(p []byte, off int64) (n int, err error) {
// 	if d.out == nil {
// 		return 0, os.ErrInvalid
// 	}

// 	// Allocate buffer for encrypted data
// 	encryptedData := make([]byte, len(p))

// 	// Encrypt the input data
// 	d.stream.XORKeyStream(encryptedData, p)

// 	// Write the encrypted data at the specified offset
// 	n, err = d.out.WriteAt(encryptedData, off)
// 	if err != nil {
// 		return 0, err
// 	}

// 	return n, nil
// }

// Close closes the input and output files
func (d *DecryptPartReader) Close() error {
	if err := d.file.Close(); err != nil {
		return err
	}
	// if d.out != nil {
	// 	return d.out.Close()
	// }
	return nil
}
