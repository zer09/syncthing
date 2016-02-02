// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package db

import (
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syndtr/goleveldb/leveldb"
)

// A readOnlyTransaction represents a database snapshot.
type readOnlyTransaction struct {
	*leveldb.Snapshot
	db *Instance
}

func (db *Instance) newReadOnlyTransaction() readOnlyTransaction {
	snap, err := db.GetSnapshot()
	if err != nil {
		panic(err)
	}
	return readOnlyTransaction{
		Snapshot: snap,
		db:       db,
	}
}

func (t readOnlyTransaction) close() {
	t.Release()
}

func (t readOnlyTransaction) getFile(folder uint32, device uint32, file uint64) (protocol.FileInfo, bool) {
	return getFile(t, t.db.deviceKey(folder, device, file))
}

// A readWriteTransaction is a readOnlyTransaction plus a batch for writes.
// The batch will be committed on close() or by checkFlush() if it exceeds the
// batch size.
type readWriteTransaction struct {
	readOnlyTransaction
	*leveldb.Batch
}

func (db *Instance) newReadWriteTransaction() readWriteTransaction {
	t := db.newReadOnlyTransaction()
	return readWriteTransaction{
		readOnlyTransaction: t,
		Batch:               new(leveldb.Batch),
	}
}

func (t readWriteTransaction) close() {
	if err := t.db.Write(t.Batch, nil); err != nil {
		panic(err)
	}
	t.readOnlyTransaction.close()
}

func (t readWriteTransaction) checkFlush() {
	if t.Batch.Len() > batchFlushSize {
		if err := t.db.Write(t.Batch, nil); err != nil {
			panic(err)
		}
		t.Batch.Reset()
	}
}

func (t readWriteTransaction) insertFile(folderID uint32, deviceID uint32, nameID uint64, file protocol.FileInfo) int64 {
	if file.LocalVersion == 0 {
		file.LocalVersion = clock(0)
	}

	nk := t.db.deviceKey(folderID, deviceID, nameID)
	t.Put(nk, file.MustMarshalXDR())

	return file.LocalVersion
}

// updateGlobal adds this device+version to the version list for the given
// file. If the device is already present in the list, the version is updated.
// If the file does not have an entry in the global list, it is created.
func (t readWriteTransaction) updateGlobal(folderID uint32, deviceID uint32, nameID uint64, file protocol.FileInfo, globalSize *sizeTracker) bool {
	gk := t.db.globalKey(folderID, nameID)
	svl, err := t.Get(gk, nil)
	if err != nil && err != leveldb.ErrNotFound {
		panic(err)
	}

	var fl versionList
	var oldFile protocol.FileInfo
	var hasOldFile bool
	// Remove the device from the current version list
	if svl != nil {
		err = fl.UnmarshalXDR(svl)
		if err != nil {
			panic(err)
		}

		for i := range fl.versions {
			if fl.versions[i].deviceID == deviceID {
				if fl.versions[i].version.Equal(file.Version) {
					// No need to do anything
					return false
				}

				if i == 0 {
					// Keep the current newest file around so we can subtract it from
					// the globalSize if we replace it.
					oldFile, hasOldFile = t.getFile(folderID, fl.versions[0].deviceID, nameID)
				}

				fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
				break
			}
		}
	}

	nv := fileVersion{
		deviceID: deviceID,
		version:  file.Version,
	}

	insertedAt := -1
	// Find a position in the list to insert this file. The file at the front
	// of the list is the newer, the "global".
	for i := range fl.versions {
		switch fl.versions[i].version.Compare(file.Version) {
		case protocol.Equal, protocol.Lesser:
			// The version at this point in the list is equal to or lesser
			// ("older") than us. We insert ourselves in front of it.
			fl.versions = insertVersion(fl.versions, i, nv)
			insertedAt = i
			goto done

		case protocol.ConcurrentLesser, protocol.ConcurrentGreater:
			// The version at this point is in conflict with us. We must pull
			// the actual file metadata to determine who wins. If we win, we
			// insert ourselves in front of the loser here. (The "Lesser" and
			// "Greater" in the condition above is just based on the device
			// IDs in the version vector, which is not the only thing we use
			// to determine the winner.)
			of, ok := t.getFile(folderID, fl.versions[i].deviceID, nameID)
			if !ok {
				panic("file referenced in version list does not exist")
			}
			if file.WinsConflict(of) {
				fl.versions = insertVersion(fl.versions, i, nv)
				insertedAt = i
				goto done
			}
		}
	}

	// We didn't find a position for an insert above, so append to the end.
	fl.versions = append(fl.versions, nv)
	insertedAt = len(fl.versions) - 1

done:
	if insertedAt == 0 {
		// We just inserted a new newest version. Fixup the global size
		// calculation.
		if !file.Version.Equal(oldFile.Version) {
			globalSize.addFile(file)
			if hasOldFile {
				// We have the old file that was removed at the head of the list.
				globalSize.removeFile(oldFile)
			} else if len(fl.versions) > 1 {
				// The previous newest version is now at index 1, grab it from there.
				oldFile, ok := t.getFile(folderID, fl.versions[1].deviceID, nameID)
				if !ok {
					panic("file referenced in version list does not exist")
				}
				globalSize.removeFile(oldFile)
			}
		}
	}

	l.Debugf("new global after update: %x => %v", gk, fl)
	t.Put(gk, fl.MustMarshalXDR())

	return true
}

// removeFromGlobal removes the device from the global version list for the
// given file. If the version list is empty after this, the file entry is
// removed entirely.
func (t readWriteTransaction) removeFromGlobal(folderID uint32, deviceID uint32, nameID uint64, globalSize *sizeTracker) {
	gk := t.db.globalKey(folderID, nameID)
	svl, err := t.Get(gk, nil)
	if err != nil {
		// We might be called to "remove" a global version that doesn't exist
		// if the first update for the file is already marked invalid.
		return
	}

	var fl versionList
	err = fl.UnmarshalXDR(svl)
	if err != nil {
		panic(err)
	}

	removed := false
	for i := range fl.versions {
		if fl.versions[i].deviceID == deviceID {
			if i == 0 && globalSize != nil {
				f, ok := t.getFile(folderID, deviceID, nameID)
				if !ok {
					panic("removing nonexistent file")
				}
				globalSize.removeFile(f)
				removed = true
			}
			fl.versions = append(fl.versions[:i], fl.versions[i+1:]...)
			break
		}
	}

	if len(fl.versions) == 0 {
		t.Delete(gk)
	} else {
		l.Debugf("new global after remove: %v", fl)
		t.Put(gk, fl.MustMarshalXDR())
		if removed {
			f, ok := t.getFile(folderID, fl.versions[0].deviceID, nameID)
			if !ok {
				panic("new global is nonexistent file")
			}
			globalSize.addFile(f)
		}
	}
}

func insertVersion(vl []fileVersion, i int, v fileVersion) []fileVersion {
	t := append(vl, fileVersion{})
	copy(t[i+1:], t[i:])
	t[i] = v
	return t
}
