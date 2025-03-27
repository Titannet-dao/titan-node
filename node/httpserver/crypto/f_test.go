package fscrypto

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func TestEn(t *testing.T) {
	f, err := os.Open("/Users/zt/Downloads/111.zip")
	if err != nil {
		t.Fatal(err)
	}
	pass := []byte("123456")

	fs, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	t.Log("raw file size: ", fs.Size())

	r, n, err := Encrypt(f, pass, "./")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("encrypted file size: ", n)

	// 写入文件
	w, err := os.Create(fmt.Sprintf("./%s_enc.dat", fs.Name()))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if _, err := io.Copy(w, r); err != nil {
		t.Fatal(err)
	}
}

func TestDe(t *testing.T) {
	f, err := os.Open("./111.zip_enc.dat")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	pass := []byte("123456")

	if err != nil {
		t.Fatal(err)
	}
	r, err := Decrypt(f, pass)
	if err != nil {
		t.Fatal(err)
	}
	w, err := os.Create("./111.zip")
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if _, err := io.Copy(w, r); err != nil {
		t.Fatal(err)
	}
}

func TestDePR(t *testing.T) {

	fp := "./111.zip_enc.dat"

	fs, _ := os.Stat(fp)
	dp, err := NewDecryptPartReader(fp, []byte("123456"))
	if err != nil {
		t.Fatal(err)
	}
	defer dp.Close()

	chunkSize := 1024 * 1024

	rf, err := os.Create("./111.zip")
	if err != nil {
		t.Fatal(err)
	}
	defer rf.Close()

	for i := int64(0); i < fs.Size(); i += int64(chunkSize) {
		end := i + int64(chunkSize) - 1
		if end >= fs.Size() {
			end = fs.Size() - 1
		}
		t.Log("start: ", i, "end: ", end)
		buf := make([]byte, chunkSize)
		n, err := dp.ReadAt(buf, i)

		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if n > 0 {
			rf.Write(buf[:n])
		}
		t.Log("read size: ", n)
	}

	if err := rf.Sync(); err != nil {
		t.Fatal(err)
	}

}
