// Copyright (C) 2016 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package db

import (
	"encoding/binary"

	"github.com/syncthing/syncthing/lib/sync"
	"github.com/syndtr/goleveldb/leveldb"
)

// A largeIndex is an in memory bidirectional []byte to uint64 map. It is
// database backed.
type largeIndex struct {
	db        *Instance
	keyPrefix []byte
	valPrefix []byte
	nextID    uint64
	keyBuf    []byte
	mut       sync.Mutex
}

func newLargeIndex(db *Instance, prefix []byte) *largeIndex {
	idx := &largeIndex{
		db:        db,
		keyPrefix: append(prefix, 0),
		valPrefix: append(prefix, 1),
		mut:       sync.NewMutex(),
	}
	return idx
}

// ID returns the index number for the given byte slice, allocating a new one
// and persisting this to the database if necessary.
func (i *largeIndex) ID(val []byte) uint64 {
	i.mut.Lock()
	defer i.mut.Unlock()

	i.keyBuf = append(i.keyBuf[:0], i.valPrefix...)
	i.keyBuf = append(i.keyBuf, val...)
	bs, err := i.db.Get(i.keyBuf, nil)
	if err == nil {
		return binary.BigEndian.Uint64(bs)
	}
	if err != leveldb.ErrNotFound {
		panic(err)
	}

	id := i.nextID
	i.nextID++

	idBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(idBuf, id)

	// val -> id
	// key is already {valPrefix, val}
	if err := i.db.Put(i.keyBuf, idBuf, nil); err != nil {
		panic(err)
	}

	// val -> id
	i.keyBuf = append(i.keyBuf[:0], i.keyPrefix...)
	i.keyBuf = append(i.keyBuf, idBuf...)
	if err := i.db.Put(i.keyBuf, val, nil); err != nil {
		panic(err)
	}

	return id
}

// Val returns the value for the given index number, or (nil, false) if there
// is no such index number.
func (i *largeIndex) Val(id uint64) ([]byte, bool) {
	idBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(idBuf, id)
	key := append(i.keyPrefix, idBuf...)

	bs, err := i.db.Get(key, nil)
	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	return bs, err == nil
}
