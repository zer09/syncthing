// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:generate -command genxdr go run ../../Godeps/_workspace/src/github.com/calmh/xdr/cmd/genxdr/main.go
//go:generate genxdr -o leveldb_xdr.go leveldb.go

package db

import (
	"bytes"
	"fmt"

	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/sync"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
)

var (
	clockTick int64
	clockMut  = sync.NewMutex()
)

func clock(v int64) int64 {
	clockMut.Lock()
	defer clockMut.Unlock()
	if v > clockTick {
		clockTick = v + 1
	} else {
		clockTick++
	}
	return clockTick
}

// We use static numbering here instead of iota as we can't allow these
// numbers to change if we, for example, remove a key type.
const (
	KeyTypeDevice          = 0 // <folder ID><device ID><file name> => FileInfo
	KeyTypeGlobal          = 1 // <folder ID><file name> => versionList
	KeyTypeBlock           = 2 // <folder ID><hash><file name> => block index (uint32)
	KeyTypeDeviceStatistic = 3
	KeyTypeFolderStatistic = 4
	KeyTypeVirtualMtime    = 5 // <folder ID><file name> => {time.Time, time.Time}
	KeyTypeFolderIdx       = 6 // <folder ID> => folder (string)
	KeyTypeDeviceIdx       = 7 // <device ID> => device (string)
	KeyTypeNameIdx         = 8 // [0x00]<name ID> => file name, [0x01]<file name> => name ID (uint64)
)

type fileVersion struct {
	version  protocol.Vector
	deviceID uint32
}

type versionList struct {
	versions []fileVersion
}

// to deserialize v0.12 database format
type oldFileVersion struct {
	version protocol.Vector
	device  []byte
}

// to deserialize v0.12 database format
type oldVersionList struct {
	versions []oldFileVersion
}

func (l versionList) String() string {
	var b bytes.Buffer
	b.WriteString("[")
	for _, v := range l.versions {
		fmt.Fprintf(&b, "[%d, %d]", v.version, v.deviceID)
	}
	b.WriteString("]")
	return b.String()
}

type fileList []protocol.FileInfo

func (l fileList) Len() int {
	return len(l)
}

func (l fileList) Swap(a, b int) {
	l[a], l[b] = l[b], l[a]
}

func (l fileList) Less(a, b int) bool {
	return l[a].Name < l[b].Name
}

type dbReader interface {
	Get([]byte, *opt.ReadOptions) ([]byte, error)
}

// Flush batches to disk when they contain this many records.
const batchFlushSize = 64

func getFile(db dbReader, key []byte) (protocol.FileInfo, bool) {
	bs, err := db.Get(key, nil)
	if err == leveldb.ErrNotFound {
		return protocol.FileInfo{}, false
	}
	if err != nil {
		panic(err)
	}

	var f protocol.FileInfo
	err = f.UnmarshalXDR(bs)
	if err != nil {
		panic(err)
	}
	return f, true
}
