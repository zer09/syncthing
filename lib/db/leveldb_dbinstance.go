// Copyright (C) 2014 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package db

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/syncthing/syncthing/lib/osutil"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/errors"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
	"github.com/syndtr/goleveldb/leveldb/util"
)

type deletionHandler func(t readWriteTransaction, folder, device, name []byte, dbi iterator.Iterator) int64

type Instance struct {
	*leveldb.DB
	folderIdx *smallIndex
	deviceIdx *smallIndex
	nameIdx   *largeIndex
}

const (
	keyPrefixLen = 1
	keyFolderLen = 4 // indexed
	keyDeviceLen = 4 // indexed
	keyNameLen   = 8 // indexed
	keyHashLen   = 32
)

func Open(file string) (*Instance, error) {
	opts := &opt.Options{
		OpenFilesCacheCapacity: 100,
		WriteBuffer:            4 << 20,
	}

	if _, err := os.Stat(file); os.IsNotExist(err) {
		// The file we are looking to open does not exist. This may be the
		// first launch so we should look for an old version and try to
		// convert it.
		if err := checkConvertDatabase(file); err != nil {
			l.Infoln("Converting old database:", err)
			l.Infoln("Will rescan from scratch.")
		}
	}

	db, err := leveldb.OpenFile(file, opts)
	if leveldbIsCorrupted(err) {
		db, err = leveldb.RecoverFile(file, opts)
	}
	if leveldbIsCorrupted(err) {
		// The database is corrupted, and we've tried to recover it but it
		// didn't work. At this point there isn't much to do beyond dropping
		// the database and reindexing...
		l.Infoln("Database corruption detected, unable to recover. Reinitializing...")
		if err := os.RemoveAll(file); err != nil {
			return nil, err
		}
		db, err = leveldb.OpenFile(file, opts)
	}
	if err != nil {
		return nil, err
	}

	return newDBInstance(db), nil
}

func OpenMemory() *Instance {
	db, _ := leveldb.Open(storage.NewMemStorage(), nil)
	return newDBInstance(db)
}

func newDBInstance(db *leveldb.DB) *Instance {
	i := &Instance{
		DB: db,
	}
	i.folderIdx = newSmallIndex(i, []byte{KeyTypeFolderIdx})
	i.deviceIdx = newSmallIndex(i, []byte{KeyTypeDeviceIdx})
	i.nameIdx = newLargeIndex(i, []byte{KeyTypeNameIdx})
	return i
}

func (db *Instance) Compact() error {
	return db.CompactRange(util.Range{})
}

func (db *Instance) replace(folder, device []byte, fs []protocol.FileInfo, localSize, globalSize *sizeTracker) int64 {
	files := make(map[string]protocol.FileInfo)
	for _, f := range fs {
		files[f.Name] = f
	}

	t := db.newReadWriteTransaction()
	defer t.close()

	folderID := db.folderIdx.ID(folder)
	deviceID := db.deviceIdx.ID(device)

	dbi := t.NewIterator(util.BytesPrefix(db.deviceKey(folderID, deviceID, 0)[:keyPrefixLen+keyFolderLen+keyDeviceLen]), nil)
	defer dbi.Release()

	var maxLocalVer int64
	isLocalDevice := bytes.Equal(device, protocol.LocalDeviceID[:])

	for dbi.Next() {
		var ef FileInfoTruncated
		nameID := db.deviceKeyNameID(dbi.Key())
		name, _ := db.nameIdx.Val(nameID)
		nameStr := string(name)
		newF, ok := files[nameStr]

		if ok {
			// File exists on both sides - compare versions. We might get an
			// update with the same version and different flags if a device has
			// marked a file as invalid, so handle that too.
			l.Debugln("generic replace; exists - compare", nameStr)
			ef.UnmarshalXDR(dbi.Value())
			if !newF.Version.Equal(ef.Version) || newF.Flags != ef.Flags {
				l.Debugln("generic replace; differs - insert")
				if lv := t.insertFile(folderID, deviceID, nameID, newF); lv > maxLocalVer {
					maxLocalVer = lv
				}
				if isLocalDevice {
					localSize.removeFile(ef)
					localSize.addFile(newF)
				}
				if newF.IsInvalid() {
					t.removeFromGlobal(folderID, deviceID, nameID, globalSize)
				} else {
					t.updateGlobal(folderID, deviceID, nameID, newF, globalSize)
				}
			} else {
				l.Debugln("generic replace; equal - ignore")
			}
			delete(files, nameStr)
		} else {
			// File exists in database but not in the map of new files.
			l.Debugln("generic replace; exists - remove", nameStr)
			t.removeFromGlobal(folderID, deviceID, nameID, globalSize)
			t.Delete(dbi.Key())
			localSize.removeFile(ef)
		}

		// Write out and reuse the batch every few records, to avoid the batch
		// growing too large and thus allocating unnecessarily much memory.
		t.checkFlush()
	}

	// Add any remaining files
	for name, newF := range files {
		nameID := db.nameIdx.ID([]byte(name))
		l.Debugln("generic replace; new - insert", name)
		if lv := t.insertFile(folderID, deviceID, nameID, newF); lv > maxLocalVer {
			maxLocalVer = lv
		}
		if isLocalDevice {
			localSize.addFile(newF)
		}
		if newF.IsInvalid() {
			t.removeFromGlobal(folderID, deviceID, nameID, globalSize)
		} else {
			t.updateGlobal(folderID, deviceID, nameID, newF, globalSize)
		}
	}

	return maxLocalVer
}

func (db *Instance) updateFiles(folder, device []byte, fs []protocol.FileInfo, localSize, globalSize *sizeTracker) int64 {
	t := db.newReadWriteTransaction()
	defer t.close()

	folderID := db.folderIdx.ID(folder)
	deviceID := db.deviceIdx.ID(device)

	var maxLocalVer int64
	var fk []byte
	isLocalDevice := bytes.Equal(device, protocol.LocalDeviceID[:])
	for _, f := range fs {
		name := []byte(f.Name)
		nameID := db.nameIdx.ID(name)
		fk = db.deviceKeyInto(fk[:cap(fk)], folderID, deviceID, nameID)
		bs, err := t.Get(fk, nil)
		if err == leveldb.ErrNotFound {
			if isLocalDevice {
				localSize.addFile(f)
			}

			if lv := t.insertFile(folderID, deviceID, nameID, f); lv > maxLocalVer {
				maxLocalVer = lv
			}
			if f.IsInvalid() {
				t.removeFromGlobal(folderID, deviceID, nameID, globalSize)
			} else {
				t.updateGlobal(folderID, deviceID, nameID, f, globalSize)
			}
			continue
		}

		var ef FileInfoTruncated
		err = ef.UnmarshalXDR(bs)
		if err != nil {
			panic(err)
		}
		// Flags might change without the version being bumped when we set the
		// invalid flag on an existing file.
		if !ef.Version.Equal(f.Version) || ef.Flags != f.Flags {
			if isLocalDevice {
				localSize.removeFile(ef)
				localSize.addFile(f)
			}

			if lv := t.insertFile(folderID, deviceID, nameID, f); lv > maxLocalVer {
				maxLocalVer = lv
			}
			if f.IsInvalid() {
				t.removeFromGlobal(folderID, deviceID, nameID, globalSize)
			} else {
				t.updateGlobal(folderID, deviceID, nameID, f, globalSize)
			}
		}

		// Write out and reuse the batch every few records, to avoid the batch
		// growing too large and thus allocating unnecessarily much memory.
		t.checkFlush()
	}

	return maxLocalVer
}

func (db *Instance) withHave(folder, device []byte, truncate bool, fn Iterator) {
	t := db.newReadOnlyTransaction()
	defer t.close()

	folderID := db.folderIdx.ID(folder)
	deviceID := db.deviceIdx.ID(device)

	dbi := t.NewIterator(util.BytesPrefix(db.deviceKey(folderID, deviceID, 0)[:keyPrefixLen+keyFolderLen+keyDeviceLen]), nil)
	defer dbi.Release()

	for dbi.Next() {
		// The iterator function may keep a reference to the unmarshalled
		// struct, which in turn references the buffer it was unmarshalled
		// from. dbi.Value() just returns an internal slice that it reuses, so
		// we need to copy it.
		f, err := unmarshalTrunc(append([]byte{}, dbi.Value()...), truncate)
		if err != nil {
			panic(err)
		}
		if cont := fn(f); !cont {
			return
		}
	}
}

func (db *Instance) withAllFolderTruncated(folder []byte, fn func(device []byte, f FileInfoTruncated) bool) {
	t := db.newReadWriteTransaction()
	defer t.close()

	folderID := db.folderIdx.ID(folder)

	dbi := t.NewIterator(util.BytesPrefix(db.deviceKey(folderID, 0, 0)[:keyPrefixLen+keyFolderLen]), nil)
	defer dbi.Release()

	for dbi.Next() {
		device := db.deviceKeyDevice(dbi.Key())
		var f FileInfoTruncated
		// The iterator function may keep a reference to the unmarshalled
		// struct, which in turn references the buffer it was unmarshalled
		// from. dbi.Value() just returns an internal slice that it reuses, so
		// we need to copy it.
		err := f.UnmarshalXDR(append([]byte{}, dbi.Value()...))
		if err != nil {
			panic(err)
		}

		if cont := fn(device, f); !cont {
			return
		}
	}
}

func (db *Instance) getFile(folderID uint32, deviceID uint32, nameID uint64) (protocol.FileInfo, bool) {
	return getFile(db, db.deviceKey(folderID, deviceID, nameID))
}

func (db *Instance) getGlobal(folderID uint32, nameID uint64, truncate bool) (FileIntf, bool) {
	k := db.globalKey(folderID, nameID)

	t := db.newReadOnlyTransaction()
	defer t.close()

	bs, err := t.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil, false
	}
	if err != nil {
		panic(err)
	}

	var vl versionList
	err = vl.UnmarshalXDR(bs)
	if err != nil {
		panic(err)
	}
	if len(vl.versions) == 0 {
		l.Debugln(k)
		panic("no versions?")
	}

	k = db.deviceKey(folderID, vl.versions[0].deviceID, nameID)
	bs, err = t.Get(k, nil)
	if err != nil {
		panic(err)
	}

	fi, err := unmarshalTrunc(bs, truncate)
	if err != nil {
		panic(err)
	}
	return fi, true
}

func (db *Instance) withGlobal(folder, prefix []byte, truncate bool, fn Iterator) {
	t := db.newReadOnlyTransaction()
	defer t.close()

	folderID := db.folderIdx.ID(folder)

	dbi := t.NewIterator(util.BytesPrefix(db.globalKey(folderID, 0)[:keyPrefixLen+keyFolderLen]), nil)
	defer dbi.Release()

	var fk []byte
	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugln(dbi.Key())
			panic("no versions?")
		}

		nameID := db.globalKeyNameID(dbi.Key())
		if len(prefix) > 0 {
			name, _ := db.nameIdx.Val(nameID)
			if !bytes.HasPrefix(name, prefix) {
				// The prefix filtering is rather inefficient, unfortunately.
				continue
			}
		}

		fk = db.deviceKeyInto(fk[:cap(fk)], folderID, vl.versions[0].deviceID, nameID)
		bs, err := t.Get(fk, nil)
		if err != nil {
			l.Debugf("folder: %q (%x)", folder, folder)
			l.Debugf("key: %q (%x)", dbi.Key(), dbi.Key())
			l.Debugf("vl: %v", vl)
			l.Debugf("vl.versions[0].deviceID: %x", vl.versions[0].deviceID)
			l.Debugf("name: %d (%x)", nameID, nameID)
			l.Debugf("fk: %q", fk)
			l.Debugf("fk: %x %x %x",
				fk[keyPrefixLen:keyPrefixLen+keyFolderLen],
				fk[keyPrefixLen+keyFolderLen:keyPrefixLen+keyFolderLen+keyDeviceLen],
				fk[keyPrefixLen+keyFolderLen+keyDeviceLen:])
			panic(err)
		}

		f, err := unmarshalTrunc(bs, truncate)
		if err != nil {
			panic(err)
		}

		if cont := fn(f); !cont {
			return
		}
	}
}

func (db *Instance) availability(folder, file []byte) []protocol.DeviceID {
	folderID := db.folderIdx.ID(folder)
	nameID := db.nameIdx.ID(file)

	k := db.globalKey(folderID, nameID)
	bs, err := db.Get(k, nil)
	if err == leveldb.ErrNotFound {
		return nil
	}
	if err != nil {
		panic(err)
	}

	var vl versionList
	err = vl.UnmarshalXDR(bs)
	if err != nil {
		panic(err)
	}

	var devices []protocol.DeviceID
	for _, v := range vl.versions {
		if !v.version.Equal(vl.versions[0].version) {
			break
		}
		id, _ := db.deviceIdx.Val(v.deviceID)
		n := protocol.DeviceIDFromBytes(id)
		devices = append(devices, n)
	}

	return devices
}

func (db *Instance) withNeed(folder, device []byte, truncate bool, fn Iterator) {
	folderID := db.folderIdx.ID(folder)
	deviceID := db.deviceIdx.ID(device)

	t := db.newReadOnlyTransaction()
	defer t.close()

	dbi := t.NewIterator(util.BytesPrefix(db.globalKey(folderID, 0)[:keyPrefixLen+keyFolderLen]), nil)
	defer dbi.Release()

	var fk []byte
nextFile:
	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}
		if len(vl.versions) == 0 {
			l.Debugln(dbi.Key())
			panic("no versions?")
		}

		have := false // If we have the file, any version
		need := false // If we have a lower version of the file
		for _, v := range vl.versions {
			if v.deviceID == deviceID {
				have = true
				need = !v.version.GreaterEqual(vl.versions[0].version)
				break
			}
		}

		if need || !have {
			nameID := db.globalKeyNameID(dbi.Key())
			needVersion := vl.versions[0].version

		nextVersion:
			for i := range vl.versions {
				if !vl.versions[i].version.Equal(needVersion) {
					// We haven't found a valid copy of the file with the needed version.
					continue nextFile
				}
				fk = db.deviceKeyInto(fk[:cap(fk)], folderID, vl.versions[i].deviceID, nameID)
				bs, err := t.Get(fk, nil)
				if err != nil {
					var id protocol.DeviceID
					copy(id[:], device)
					l.Debugf("device: %v", id)
					l.Debugf("need: %v, have: %v", need, have)
					l.Debugf("key: %q (%x)", dbi.Key(), dbi.Key())
					l.Debugf("vl: %v", vl)
					l.Debugf("i: %v", i)
					l.Debugf("fk: %q (%x)", fk, fk)
					l.Debugf("name: %d (%x)", nameID, nameID)
					panic(err)
				}

				gf, err := unmarshalTrunc(bs, truncate)
				if err != nil {
					panic(err)
				}

				if gf.IsInvalid() {
					// The file is marked invalid for whatever reason, don't use it.
					continue nextVersion
				}

				if gf.IsDeleted() && !have {
					// We don't need deleted files that we don't have
					continue nextFile
				}

				if cont := fn(gf); !cont {
					return
				}

				// This file is handled, no need to look further in the version list
				continue nextFile
			}
		}
	}
}

func (db *Instance) ListFolders() []string {
	t := db.newReadOnlyTransaction()
	defer t.close()

	dbi := t.NewIterator(util.BytesPrefix([]byte{KeyTypeGlobal}), nil)
	defer dbi.Release()

	folderExists := make(map[string]bool)
	for dbi.Next() {
		folder := string(db.globalKeyFolder(dbi.Key()))
		if !folderExists[folder] {
			folderExists[folder] = true
		}
	}

	folders := make([]string, 0, len(folderExists))
	for k := range folderExists {
		folders = append(folders, k)
	}

	sort.Strings(folders)
	return folders
}

func (db *Instance) dropFolder(folder []byte) {
	t := db.newReadOnlyTransaction()
	defer t.close()

	// Remove all items related to the given folder from the device->file bucket
	dbi := t.NewIterator(util.BytesPrefix([]byte{KeyTypeDevice}), nil)
	for dbi.Next() {
		itemFolder := db.deviceKeyFolder(dbi.Key())
		if bytes.Compare(folder, itemFolder) == 0 {
			db.Delete(dbi.Key(), nil)
		}
	}
	dbi.Release()

	// Remove all items related to the given folder from the global bucket
	dbi = t.NewIterator(util.BytesPrefix([]byte{KeyTypeGlobal}), nil)
	for dbi.Next() {
		itemFolder := db.globalKeyFolder(dbi.Key())
		if bytes.Compare(folder, itemFolder) == 0 {
			db.Delete(dbi.Key(), nil)
		}
	}
	dbi.Release()
}

func (db *Instance) checkGlobals(folder []byte, globalSize *sizeTracker) {
	t := db.newReadWriteTransaction()
	defer t.close()

	folderID := db.folderIdx.ID(folder)

	dbi := t.NewIterator(util.BytesPrefix(db.globalKey(folderID, 0)[:keyPrefixLen+keyFolderLen]), nil)
	defer dbi.Release()

	var fk []byte
	for dbi.Next() {
		var vl versionList
		err := vl.UnmarshalXDR(dbi.Value())
		if err != nil {
			panic(err)
		}

		// Check the global version list for consistency. An issue in previous
		// versions of goleveldb could result in reordered writes so that
		// there are global entries pointing to no longer existing files. Here
		// we find those and clear them out.

		nameID := db.globalKeyNameID(dbi.Key())
		var newVL versionList
		for i, version := range vl.versions {
			fk = db.deviceKeyInto(fk[:cap(fk)], folderID, version.deviceID, nameID)

			_, err := t.Get(fk, nil)
			if err == leveldb.ErrNotFound {
				continue
			}
			if err != nil {
				panic(err)
			}
			newVL.versions = append(newVL.versions, version)

			if i == 0 {
				fi, ok := t.getFile(folderID, version.deviceID, nameID)
				if !ok {
					panic("nonexistent global master file")
				}
				globalSize.addFile(fi)
			}
		}

		if len(newVL.versions) != len(vl.versions) {
			t.Put(dbi.Key(), newVL.MustMarshalXDR())
			t.checkFlush()
		}
	}
	l.Debugf("db check completed for %q", folder)
}

// deviceKey returns a byte slice encoding the following information:
//	   keyTypeDevice (1 byte)
//	   folder (4 bytes)
//	   device (4 bytes)
//	   name (variable size)
func (db *Instance) deviceKey(folderID uint32, deviceID uint32, nameID uint64) []byte {
	return db.deviceKeyInto(nil, folderID, deviceID, nameID)
}

func (db *Instance) deviceKeyInto(k []byte, folderID uint32, deviceID uint32, nameID uint64) []byte {
	reqLen := keyPrefixLen + keyFolderLen + keyDeviceLen + keyNameLen
	if len(k) < reqLen {
		k = make([]byte, reqLen)
	}
	k[0] = KeyTypeDevice
	binary.BigEndian.PutUint32(k[keyPrefixLen:], folderID)
	binary.BigEndian.PutUint32(k[keyPrefixLen+keyFolderLen:], deviceID)
	binary.BigEndian.PutUint64(k[keyPrefixLen+keyFolderLen+keyDeviceLen:], nameID)
	return k[:reqLen]
}

// deviceKeyName returns the file name from the key
func (db *Instance) deviceKeyName(key []byte) []byte {
	name, ok := db.nameIdx.Val(db.deviceKeyNameID(key))
	if !ok {
		panic("bug: lookup of nonexistent name ID")
	}
	return name
}

// deviceKeyNameID returns the file name ID from the key
func (db *Instance) deviceKeyNameID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[keyPrefixLen+keyFolderLen+keyDeviceLen:])
}

// deviceKeyFolder returns the folder name from the key
func (db *Instance) deviceKeyFolder(key []byte) []byte {
	folder, ok := db.folderIdx.Val(binary.BigEndian.Uint32(key[keyPrefixLen:]))
	if !ok {
		panic("bug: lookup of nonexistent folder ID")
	}
	return folder
}

// deviceKeyDevice returns the device ID from the key
func (db *Instance) deviceKeyDevice(key []byte) []byte {
	device, ok := db.deviceIdx.Val(binary.BigEndian.Uint32(key[keyPrefixLen+keyFolderLen:]))
	if !ok {
		panic("bug: lookup of nonexistent device ID")
	}
	return device
}

// globalKey returns a byte slice encoding the following information:
//	   keyTypeGlobal (1 byte)
//	   folder (4 bytes)
//	   name (variable size)
func (db *Instance) globalKey(folderID uint32, nameID uint64) []byte {
	k := make([]byte, keyPrefixLen+keyFolderLen+keyNameLen)
	k[0] = KeyTypeGlobal
	binary.BigEndian.PutUint32(k[keyPrefixLen:], folderID)
	binary.BigEndian.PutUint64(k[keyPrefixLen+keyFolderLen:], nameID)
	return k
}

// globalKeyName returns the filename from the key
func (db *Instance) globalKeyName(key []byte) []byte {
	name, ok := db.nameIdx.Val(db.globalKeyNameID(key))
	if !ok {
		panic("bug: lookup of nonexistent name ID")
	}
	return name
}

// globalKeyNameID returns the name ID from the key
func (db *Instance) globalKeyNameID(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[keyPrefixLen+keyFolderLen:])
}

// globalKeyFolder returns the folder name from the key
func (db *Instance) globalKeyFolder(key []byte) []byte {
	folder, ok := db.folderIdx.Val(binary.BigEndian.Uint32(key[keyPrefixLen:]))
	if !ok {
		panic("bug: lookup of nonexistent folder ID")
	}
	return folder
}

func unmarshalTrunc(bs []byte, truncate bool) (FileIntf, error) {
	if truncate {
		var tf FileInfoTruncated
		err := tf.UnmarshalXDR(bs)
		return tf, err
	}

	var tf protocol.FileInfo
	err := tf.UnmarshalXDR(bs)
	return tf, err
}

// A "better" version of leveldb's errors.IsCorrupted.
func leveldbIsCorrupted(err error) bool {
	switch {
	case err == nil:
		return false

	case errors.IsCorrupted(err):
		return true

	case strings.Contains(err.Error(), "corrupted"):
		return true
	}

	return false
}

// checkConvertDatabase tries to convert an existing old (v0.11) database to
// new (v0.13) format.
func checkConvertDatabase(dbFile string) error {
	oldLoc := filepath.Join(filepath.Dir(dbFile), "index-v0.11.0.db")
	if _, err := os.Stat(oldLoc); os.IsNotExist(err) {
		// The old database file does not exist; that's ok, continue as if
		// everything succeeded.
		return nil
	} else if err != nil {
		// Any other error is weird.
		return err
	}

	// There exists a database in the old format. We run a one time
	// conversion from old to new.

	fromDb, err := leveldb.OpenFile(oldLoc, nil)
	if err != nil {
		return err
	}

	toDb, err := leveldb.OpenFile(dbFile, nil)
	if err != nil {
		return err
	}

	err = convertKeyFormat(fromDb, toDb)
	if err != nil {
		return err
	}

	err = toDb.Close()
	if err != nil {
		return err
	}

	// We've done this one, we don't want to do it again (if the user runs
	// -reset or so). We don't care too much about errors any more at this stage.
	fromDb.Close()
	osutil.Rename(oldLoc, oldLoc+".converted")

	return nil
}
