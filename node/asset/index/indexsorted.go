package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"io"
	"sort"

	"github.com/ipfs/go-cid"
	"github.com/ipld/go-car/v2/index"

	// internalio "github.com/ipld/go-car/v2/internal/io"
	"github.com/multiformats/go-multicodec"
	"github.com/multiformats/go-multihash"
	"golang.org/x/xerrors"
)

const multiIndexCodec = multicodec.Code(0x400004)

type bucket struct {
	code    uint32
	records []*index.Record
}

type MultiIndexSorted struct {
	buckets map[uint32]*bucket
	size    uint32
}

// Marshal writes the Bucket struct to a writer.
func (b *bucket) Marshal(w io.Writer) (uint64, error) {
	l := uint64(0)
	if err := binary.Write(w, binary.LittleEndian, b.code); err != nil {
		return 0, err
	}
	l += 4

	if err := binary.Write(w, binary.LittleEndian, uint32(len(b.records))); err != nil {
		return l, err
	}
	l += 4

	for _, record := range b.records {
		if err := binary.Write(w, binary.LittleEndian, record.Offset); err != nil {
			return l, err
		}
		l += 8

		if err := binary.Write(w, binary.LittleEndian, uint32(len(record.Hash()))); err != nil {
			return l, err
		}
		l += 4

		n, err := w.Write(record.Hash())
		if err != nil {
			return l, err
		}
		l += uint64(n)
	}
	return l, nil
}

// Unmarshal reads the Bucket struct from a reader.
func (b *bucket) Unmarshal(r io.Reader) error {
	if err := binary.Read(r, binary.LittleEndian, &b.code); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	var count uint32
	if err := binary.Read(r, binary.LittleEndian, &count); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	for i := 0; i < int(count); i++ {
		var offset uint64
		if err := binary.Read(r, binary.LittleEndian, &offset); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		var dataLen uint32
		if err := binary.Read(r, binary.LittleEndian, &dataLen); err != nil {
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}

		buf := make([]byte, dataLen)
		if _, err := io.ReadFull(r, buf); err != nil {
			return err
		}

		b.records = append(b.records, &index.Record{Cid: cid.NewCidV1(cid.Raw, buf), Offset: offset})

	}
	return nil
}

// getAll searches for all occurrences of a CID and calls the provided function for each one.
func (b *bucket) getAll(c cid.Cid, fn func(uint64) bool) error {
	for _, record := range b.records {
		if bytes.Equal(c.Hash(), record.Hash()) {
			if !fn(record.Offset) {
				return nil
			}
		}
	}

	return xerrors.Errorf("not found")
}

// forEach iterates over all records in the bucket and calls the provided function for each one.
func (b *bucket) forEach(f func(mh multihash.Multihash, offset uint64) error) error {
	for _, record := range b.records {
		if err := f(record.Hash(), record.Offset); err != nil {
			return err
		}
	}
	return nil
}

// Codec returns the multicodec code for the MultiIndex.
func (m *MultiIndexSorted) Codec() multicodec.Code {
	return multiIndexCodec
}

// hashCode computes the hash code for a given multihash.
func (m *MultiIndexSorted) hashCode(mh multihash.Multihash) uint32 {
	hash := fnv.New32a()
	hash.Write(mh)
	return hash.Sum32() % m.size
}

// GetAll searches for all occurrences of a CID and calls the provided function for each one.
func (m *MultiIndexSorted) GetAll(c cid.Cid, fn func(uint64) bool) error {
	code := m.hashCode(c.Hash())
	if b, ok := m.buckets[code]; ok {
		return b.getAll(c, fn)
	}
	return xerrors.Errorf("not found")
}

// Marshal writes the MultiIndex struct to a writer.
func (m *MultiIndexSorted) Marshal(w io.Writer) (uint64, error) {
	l := uint64(0)
	if err := binary.Write(w, binary.LittleEndian, m.size); err != nil {
		return l, err
	}
	l += 4

	if err := binary.Write(w, binary.LittleEndian, uint32(len(m.buckets))); err != nil {
		return l, err
	}
	l += 4

	// The widths are unique, but ranging over a map isn't deterministic.
	// As per the CARv2 spec, we must order buckets by digest length.

	codes := make([]uint32, 0, len(m.buckets))
	for code := range m.buckets {
		codes = append(codes, code)
	}
	sort.Slice(codes, func(i, j int) bool {
		return codes[i] < codes[j]
	})

	for _, code := range codes {
		bucket := m.buckets[code]
		n, err := bucket.Marshal(w)
		l += n
		if err != nil {
			return l, err
		}
	}
	return l, nil
}

// Unmarshal reads the MultiIndex struct from a reader.
func (m *MultiIndexSorted) Unmarshal(r io.Reader) error {
	// reader := internalio.ToByteReadSeeker(r)
	if err := binary.Read(r, binary.LittleEndian, &m.size); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	var l int32
	if err := binary.Read(r, binary.LittleEndian, &l); err != nil {
		if err == io.EOF {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	if l < 0 {
		return errors.New("index too big; MultiIndexSorted count is overflowing int32")
	}
	for i := 0; i < int(l); i++ {
		b := &bucket{}
		if err := b.Unmarshal(r); err != nil {
			return err
		}
		m.buckets[b.code] = b
	}
	return nil
}

// Load loads a list of records into the MultiIndex.
func (m *MultiIndexSorted) Load(records []index.Record) error {
	// Group records by hash code
	byCode := make(map[uint32][]*index.Record)
	for _, record := range records {
		code := m.hashCode(record.Hash())

		recsByCode, ok := byCode[code]
		if !ok {
			recsByCode = make([]*index.Record, 0)
			byCode[code] = recsByCode
		}

		r := record
		byCode[code] = append(recsByCode, &r)
	}

	// Load each record group.
	for code, recsByCode := range byCode {
		b := &bucket{
			code:    code,
			records: recsByCode,
		}

		m.buckets[code] = b
	}

	return nil
}

// ForEach iterates over all records in the MultiIndex and calls the provided function for each one.
func (m *MultiIndexSorted) ForEach(f func(mh multihash.Multihash, offset uint64) error) error {
	codes := make([]uint32, 0, len(m.buckets))
	for k := range m.buckets {
		codes = append(codes, k)
	}
	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })
	for _, code := range codes {
		bucket := (m.buckets)[code]
		if err := bucket.forEach(f); err != nil {
			return err
		}
	}
	return nil
}

// BucketCount returns the number of buckets in the MultiIndex.
func (m *MultiIndexSorted) BucketCount() uint32 {
	return uint32(len(m.buckets))
}

// TotalRecordCount returns the total number of records in the MultiIndex.
func (m *MultiIndexSorted) TotalRecordCount() uint32 {
	count := 0
	for _, b := range m.buckets {
		count += len(b.records)
	}

	return uint32(count)
}

// GetBucketRecords returns the records of a specific bucket by its index.
func (m *MultiIndexSorted) GetBucketRecords(index uint32) (uint32, []*index.Record, error) {
	if int(index) >= len(m.buckets) {
		return 0, nil, xerrors.Errorf("index %d out bucket size", index)
	}
	codes := make([]uint32, 0, len(m.buckets))
	for k := range m.buckets {
		codes = append(codes, k)
	}

	sort.Slice(codes, func(i, j int) bool { return codes[i] < codes[j] })

	code := codes[index]
	return code, m.buckets[code].records, nil
}

// NewMultiIndexSorted creates a new MultiIndexSorted with the specified bucket size.
func NewMultiIndexSorted(sizeOfBucket uint32) *MultiIndexSorted {
	return &MultiIndexSorted{buckets: make(map[uint32]*bucket), size: sizeOfBucket}
}
