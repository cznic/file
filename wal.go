// Copyright 2017 The File Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package file

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"
	"unsafe"

	"github.com/cznic/internal/buffer"
)

const (
	bit63 = -1 << 63
	szNfo = unsafe.Sizeof(nfo{})
)

var (
	_ File        = (*WAL)(nil)
	_ io.ReaderAt = (*WAL)(nil)
	_ io.WriterAt = (*WAL)(nil)
	_ os.FileInfo = (*fileInfo)(nil)

	tag = []byte("8019cb57a7fd1ab4dacdc44d1a63fc37")
)

type nfo struct {
	pageSize int64
	pages    int64
	size     int64
	skip     int64

	// Keep last
	tag [16]byte
}

type fileInfo WAL

func (f *fileInfo) IsDir() bool        { return false }
func (f *fileInfo) ModTime() time.Time { return f.modTime }
func (f *fileInfo) Mode() os.FileMode  { return f.fileMode }
func (f *fileInfo) Name() string       { return f.name }
func (f *fileInfo) Size() int64        { return f.size }
func (f *fileInfo) Sys() interface{}   { return f.sys }

// WAL implements a write ahead log of F using W. Call F.ReadAt to perform
// 'read-commited' reads.  Call ReadAt of the WAL itself to perform 'read
// uncommitted' reads.
//
// WAL methods are not safe for concurrent use by multiple goroutines.  Callers
// must provide their own synchronization when it's used concurrently by
// multiple goroutines.
type WAL struct {
	F        File // The f argument of NewWAL for convenience. R/O
	W        File // The w argument of NewWAL for convenience. R/O
	b8       [szInt64]byte
	fileMode os.FileMode
	m        map[int64]int64 // foff: woff
	modTime  time.Time
	name     string
	nfo      [unsafe.Sizeof(nfo{})]byte
	pageMask int64
	pageSize int
	size     int64
	size0    int64
	skip     int64
	sys      interface{}
}

// NewWAL returns a newly created WAL or an error, if any. The f argument is
// the File to which the writes to WAL will eventually be committed. The w
// argument is a File used to collect the writes to the WAL before commit. The
// skip argument offsets the usage of w by skip bytes allowing for bookkeeping,
// header etc.  The pageLog argument is the binary logarithm of WAL page size.
// Passing pageLog less than 1 will panic.  The f and w arguments must not
// represent the same entity.
//
// If w contains a valid write-ahead log, it's first committed to f and
// emptied. If w contains an invalid or unreadable write ahead log, the
// function returns an error.
func NewWAL(f, w File, skip int64, pageLog int) (*WAL, error) {
	if pageLog < 1 {
		panic(fmt.Errorf("NewWAL: invalid pageLog %v", pageLog))
	}

	pageSize := 1 << uint(pageLog)
	if pageSize == 0 {
		panic(fmt.Errorf("NewWAL: invalid pageLog %v", pageLog))
	}

	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}

	r := &WAL{
		F:        f,
		W:        w,
		fileMode: fi.Mode(),
		m:        map[int64]int64{},
		modTime:  fi.ModTime(),
		name:     fi.Name(),
		pageMask: int64(pageSize) - 1,
		pageSize: pageSize,
		size0:    fi.Size(),
		size:     fi.Size(),
		skip:     skip,
		sys:      fi.Sys(),
	}
	binary.BigEndian.PutUint64(r.nfo[unsafe.Offsetof(nfo{}.pageSize):], uint64(pageSize))
	binary.BigEndian.PutUint64(r.nfo[unsafe.Offsetof(nfo{}.skip):], uint64(skip))
	copy(r.nfo[unsafe.Offsetof(nfo{}.tag):], tag)
	if fi, err = w.Stat(); err != nil {
		return nil, err
	}

	switch sz := fi.Size(); {
	case sz <= skip:
		// WAL empty, nop.
	case sz <= skip+int64(szNfo):
		return nil, fmt.Errorf("NewWAL: invalid WAL size: %#x", sz)
	default:
		var nfo nfo
		var b [szNfo]byte
		n, err := w.ReadAt(b[:], skip)
		if err != nil && err != io.EOF {
			return nil, fmt.Errorf("NewWAL: %v", err)
		}

		if n != len(b) {
			panic("internal error")
		}

		if err := binary.Read(bytes.NewBuffer(b[:]), binary.BigEndian, &nfo); err != nil {
			return nil, fmt.Errorf("NewWAL: %v", err)
		}

		if g, e := nfo.tag[:], tag; !bytes.Equal(g, e) {
			return nil, fmt.Errorf("NewWAL: WAL tag %x, expected %x", g, e)
		}

		if g, e := nfo.pageSize, int64(pageSize); g != e {
			return nil, fmt.Errorf("NewWAL: WAL page size %#x, expected %v", g, e)
		}

		if g, e := nfo.skip, skip; g != e {
			return nil, fmt.Errorf("NewWAL: WAL skip %#x, expected %v", g, e)
		}

		if sz-skip-int64(len(b))%int64(pageSize) != 0 {
			return nil, fmt.Errorf("NewWAL: invalid size of WAL: %#x", sz)
		}

		if err = r.commit(sz - int64(len(b))); err != nil {
			return nil, err
		}
	}
	return r, nil
}

// Close does nothing and returns nil.
//
// Close implements File.
func (w *WAL) Close() error { return nil }

// Stat implements File. The size reported by the result is that of w.F _after_
// w.Commit will be eventually successfully performed.
//
// Sync implements File.
func (w *WAL) Stat() (os.FileInfo, error) { return (*fileInfo)(w), nil }

// Sync executes w.W.Sync.
//
// Sync implements File.
func (w *WAL) Sync() error { return w.W.Sync() }

// Truncate changes the size of the File represented by w to the size argument.
// Size of w.F is not changed. Truncate instead changes w's metadata.
//
// Truncate implements File.
func (w *WAL) Truncate(size int64) error {
	first := size &^ w.pageMask
	zf := int64(-1)
	if po := size & w.pageMask; po != 0 {
		if wp, ok := w.m[first]; ok && wp >= 0 {
			zf = first
			z := buffer.CGet(w.pageSize - int(po))
			if _, err := w.W.WriteAt(*z, wp+po+szInt64); err != nil {
				buffer.Put(z)
				return err
			}

			buffer.Put(z)
		}

		first++
	}
	last := w.size &^ w.pageMask
	if po := w.size & w.pageMask; po != 0 && last != zf {
		last++
	}
	for ; first <= last; first++ {
		if wp, ok := w.m[first]; ok && wp >= 0 {
			w.m[first] = bit63 | wp // Invalidate the page but keep it allocated.
		}
	}

	w.size = size
	if size < w.size0 {
		w.size0 = size
	}
	return nil
}

// ReadAt performs a read-uncommitted operation on w. The semantics are those
// of io.ReaderAt.ReadAt. Call w.F.ReadAt to perform a read-committed
// operation.
//
// ReadAt implements File.
func (w *WAL) ReadAt(b []byte, off int64) (n int, err error) {
	avail := w.size - off
	for len(b) != 0 && avail != 0 {
		p := off &^ w.pageMask
		o := off & w.pageMask
		rq := w.pageSize - int(o)
		if rq > len(b) {
			rq = len(b)
		}
		if int64(rq) > avail {
			rq = int(avail)
		}
		var nr int
		switch wp, ok := w.m[p]; {
		case wp < 0:
			if off >= w.size {
				return n, io.EOF
			}

			nr = w.pageSize - int(o)
			if nr > len(b) {
				nr = len(b)
			}
			z := b[:nr]
			for i := range z {
				z[i] = 0
			}
		case ok:
			if nr, err = w.W.ReadAt(b[:rq], wp+o+szInt64); err != nil {
				if err != io.EOF {
					return n, err
				}

				err = nil
			}
		default:
			if avail0 := w.size0 - off; avail0 > 0 {
				rq0 := w.pageSize - int(o)
				if rq0 > len(b) {
					rq0 = len(b)
				}
				if rq0 > rq {
					rq0 = rq
				}
				if int64(rq0) > avail0 {
					rq0 = int(avail0)
				}
				if nr, err = w.F.ReadAt(b[:rq0], p+o); err != nil {
					if err != io.EOF {
						return n, err
					}

					err = nil
				}

				z := b[rq0:rq]
				for i := range z {
					z[i] = 0
				}
				nr += len(z)
				break
			}

			z := b[:rq]
			for i := range z {
				z[i] = 0
			}
			nr = len(z)
		}
		n += nr
		b = b[nr:]
		off += int64(nr)
		avail -= int64(nr)
	}
	if avail == 0 {
		err = io.EOF
	}
	return n, err
}

// WriteAt performs a write operation on w. The semantics are those of
// io.WriteAT.WriteAt. WriteAt does not write to w.F. Instead the writes are
// collected in w.W until committed.
//
// WriteAt implements File.
func (w *WAL) WriteAt(b []byte, off int64) (n int, err error) {
	if len(b) == 0 {
		return 0, nil
	}

	var buf []byte
	for len(b) != 0 {
		p := off &^ w.pageMask
		o := off & w.pageMask
		var nw int
		switch wp, ok := w.m[p]; {
		case wp < 0:
			if buf == nil {
				p := buffer.Get(w.pageSize + szInt64)
				defer buffer.Put(p)
				buf = *p
			}
			buf := buf[szInt64:]
			z := buf[:o]
			for i := range z {
				z[i] = 0
			}
			wp &^= bit63
			nw = copy(buf[o:], b)
			z = buf[int(o)+nw:]
			for i := range z {
				z[i] = 0
			}
			if _, err := w.W.WriteAt(buf, wp+szInt64); err != nil {
				return n, err
			}

			w.m[p] = wp
		case ok:
			rq := w.pageSize - int(o)
			if rq > len(b) {
				rq = len(b)
			}
			if nw, err = w.W.WriteAt(b[:rq], wp+o+szInt64); err != nil {
				return n, err
			}
		default:
			if buf == nil {
				p := buffer.Get(w.pageSize + szInt64)
				defer buffer.Put(p)
				buf = *p
			}
			binary.BigEndian.PutUint64(buf, uint64(p))
			rq := 0
			if p < w.size0 {
				avail := w.size0 - p
				rq = w.pageSize
				if avail < int64(w.pageSize) {
					rq = int(avail)
				}
				nr, err := w.F.ReadAt(buf[szInt64:rq+szInt64], p)
				if err != nil && err != io.EOF {
					return n, err
				}

				if nr == 0 {
					return n, io.EOF
				}
			}
			z := buf[rq+szInt64:]
			for i := range z {
				z[i] = 0
			}
			nw = copy(buf[o+szInt64:], b)
			wp = w.skip + int64(len(w.m))*int64(len(buf))
			if _, err := w.W.WriteAt(buf, wp); err != nil {
				return n, err
			}

			w.m[p] = wp
		}
		n += nw
		b = b[nw:]
		off += int64(nw)
	}
	if off > w.size {
		w.size = off
	}
	return n, nil
}

// Commit transfers all writes to w collected so far into w.F and empties w and
// w.W or returns an error, if any. If the program crashes during committing a
// subsequent NewWAL call with the same files f and w will re-initiate the
// commit operation.
//
// The WAL is ready for reuse if Commit returns nil.
func (w *WAL) Commit() error {
	for p, wp := range w.m {
		if p < w.size && wp < 0 {
			binary.BigEndian.PutUint64(w.b8[:], uint64(bit63|p))
			if _, err := w.W.WriteAt(w.b8[:], wp&^bit63); err != nil {
				return err
			}
		}
	}
	binary.BigEndian.PutUint64(w.nfo[unsafe.Offsetof(nfo{}.pages):], uint64(len(w.m)))
	binary.BigEndian.PutUint64(w.nfo[unsafe.Offsetof(nfo{}.size):], uint64(w.size))
	h := w.skip + int64(len(w.m))*int64(w.pageSize+szInt64)
	if _, err := w.W.WriteAt(w.nfo[:], h); err != nil {
		return fmt.Errorf("%T.Commit: write WAL metadata: %v", w, err)
	}

	if err := w.W.Sync(); err != nil {
		return fmt.Errorf("%T.Commit: sync WAL: %v", w, err)
	}

	if err := w.F.Truncate(w.size0); err != nil {
		return fmt.Errorf("%T.commit: truncate: %v", w, err)
	}

	return w.commit(h)
}

func (w *WAL) commit(h int64) error {
	bufSz := w.pageSize + szInt64
	p := buffer.Get(bufSz)
	defer buffer.Put(p)
	buf := *p
	for o := w.skip; o < h; o += int64(bufSz) {
		if _, err := w.W.ReadAt(buf, o); err != nil {
			return fmt.Errorf("%T.commit: read WAL: %v", w, err)
		}

		p := int64(binary.BigEndian.Uint64(buf))
		q := p &^ bit63
		if q >= w.size {
			continue
		}

		rq := w.pageSize
		avail := w.size - q
		if int64(rq) > avail {
			rq = int(avail)
		}
		if p < 0 {
			z := buf[szInt64 : szInt64+rq]
			for i := range z {
				z[i] = 0
			}
		}
		if _, err := w.F.WriteAt(buf[szInt64:szInt64+rq], q); err != nil {
			return fmt.Errorf("%T.commit: write: %v", w, err)
		}
	}
	if err := w.F.Truncate(w.size); err != nil {
		return fmt.Errorf("%T.commit: truncate: %v", w, err)
	}

	if err := w.F.Sync(); err != nil {
		return fmt.Errorf("%T.commit: sync: %v", w, err)
	}

	if err := w.W.Truncate(w.skip); err != nil {
		return fmt.Errorf("%T.commit: truncate WAL: %v", w, err)
	}

	if err := w.W.Sync(); err != nil {
		return fmt.Errorf("%T.commit: sync WAL: %v", w, err)
	}

	switch {
	case len(w.m) <= 10:
		for k := range w.m {
			delete(w.m, k)
		}
	default:
		w.m = map[int64]int64{}
	}
	w.size0 = w.size
	return nil
}
