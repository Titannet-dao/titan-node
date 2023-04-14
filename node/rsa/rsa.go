package rsa

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"hash"
)

// RsaEncryption represents an RSA encryption scheme.
type Rsa struct {
	code crypto.Hash
	hash hash.Hash
}

// NewRsaEncryption creates a new RsaEncryption instance.
func New(code crypto.Hash, hash hash.Hash) *Rsa {
	return &Rsa{code, hash}
}

// GeneratePrivateKey generates an RSA private key.
func GeneratePrivateKey(bits int) (*rsa.PrivateKey, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, bits)
	if err != nil {
		return nil, err
	}

	return privateKey, nil
}

// VerifySignature verifies an RSA signature.
func (r *Rsa) VerifySign(publicKey *rsa.PublicKey, sign []byte, content []byte) error {
	r.hash.Write(content)
	hashSum := r.hash.Sum(nil)
	r.hash.Reset()

	err := rsa.VerifyPKCS1v15(publicKey, r.code, hashSum, sign)
	if err != nil {
		return err
	}
	return nil
}

// Sign signs a message using an RSA private key.
func (r *Rsa) Sign(privateKey *rsa.PrivateKey, content []byte) ([]byte, error) {
	r.hash.Write(content)
	sum := r.hash.Sum(nil)
	r.hash.Reset()

	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, r.code, sum)
	if err != nil {
		return nil, err
	}
	return signature, nil
}

// PemToPublicKey converts a PEM-encoded public key to an *rsa.PublicKey.
func Pem2PublicKey(publicPem []byte) (*rsa.PublicKey, error) {
	block, _ := pem.Decode(publicPem)
	if block == nil {
		return nil, fmt.Errorf("failed to decode public key")
	}

	pub, err := x509.ParsePKCS1PublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key, %s", err.Error())
	}

	return pub, nil
}

// PemToPrivateKey converts a PEM-encoded private key to an *rsa.PrivateKey.
func Pem2PrivateKey(privateKeyStr []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(privateKeyStr)
	if block == nil {
		return nil, fmt.Errorf("failed to decode private key")
	}

	privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key")
	}

	return privateKey, nil
}

// PrivateKeyToPem converts an *rsa.PrivateKey to a PEM-encoded private key.
func PrivateKey2Pem(privateKey *rsa.PrivateKey) []byte {
	if privateKey == nil {
		return nil
	}

	private := x509.MarshalPKCS1PrivateKey(privateKey)

	privateKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PRIVATE KEY",
			Bytes: private,
		},
	)

	return privateKeyBytes
}

// PublicKeyToPem converts an *rsa.PublicKey to a PEM-encoded public key.
func PublicKey2Pem(publicKey *rsa.PublicKey) []byte {
	if publicKey == nil {
		return nil
	}

	public := x509.MarshalPKCS1PublicKey(publicKey)

	publicKeyBytes := pem.EncodeToMemory(
		&pem.Block{
			Type:  "RSA PUBLIC KEY",
			Bytes: public,
		},
	)

	return publicKeyBytes
}

// Encrypt encrypts data with public key
// public key bits Must be 1024 or above
func (r *Rsa) Encrypt(msg []byte, pub *rsa.PublicKey) ([]byte, error) {
	msgLen := len(msg)
	step := pub.Size() - 2*r.hash.Size() - 2
	var encryptedBytes []byte

	for start := 0; start < msgLen; start += step {
		finish := start + step
		if finish > msgLen {
			finish = msgLen
		}

		encryptedBlockBytes, err := rsa.EncryptOAEP(r.hash, rand.Reader, pub, msg[start:finish], nil)
		if err != nil {
			return nil, err
		}

		encryptedBytes = append(encryptedBytes, encryptedBlockBytes...)
	}

	return encryptedBytes, nil
}

// Decrypt decrypts data with private key
func (r *Rsa) Decrypt(ciphertext []byte, priv *rsa.PrivateKey) ([]byte, error) {
	msgLen := len(ciphertext)
	step := priv.PublicKey.Size()
	var decryptedBytes []byte

	for start := 0; start < msgLen; start += step {
		finish := start + step
		if finish > msgLen {
			finish = msgLen
		}

		decryptedBlockBytes, err := rsa.DecryptOAEP(r.hash, rand.Reader, priv, ciphertext[start:finish], nil)
		if err != nil {
			return nil, err
		}

		decryptedBytes = append(decryptedBytes, decryptedBlockBytes...)
	}

	return decryptedBytes, nil
}
