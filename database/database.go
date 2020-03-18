// Copyright 2020 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package database

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/rjl493456442/ethflare/types"
)

// NewDatabase returns a database handler by given path.
func NewDatabase(datadir string) (ethdb.Database, error) {
	if datadir == "" {
		return rawdb.NewMemoryDatabase(), nil
	}
	ldb, err := rawdb.NewLevelDBDatabase(datadir, 1024, 512, "tile")
	if err != nil {
		return nil, err
	}
	return ldb, nil
}

// Database scheme definition
var (
	headStateKey = []byte("LatestState") // headStateKey tracks the latest committed state root
	tilePrefix   = []byte("-t")          // tilePrefix + tilehash => tile
)

func WriteTile(db ethdb.KeyValueWriter, hash common.Hash, t *types.Tile) {
	blob, err := rlp.EncodeToBytes(t)
	if err != nil {
		log.Crit("Failed to encode tile", "error", err)
	}
	err = db.Put(append(tilePrefix, hash.Bytes()...), blob)
	if err != nil {
		log.Crit("Failed to write tile", "error", err)
	}
}

func ReadTile(db ethdb.KeyValueReader, hash common.Hash) (*types.Tile, error) {
	blob, err := db.Get(append(tilePrefix, hash.Bytes()...))
	if err != nil {
		return nil, err
	}
	var tile types.Tile
	if err := rlp.DecodeBytes(blob, &tile); err != nil {
		return nil, err
	}
	return &tile, nil
}

func DeleteTile(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Delete(append(tilePrefix, hash.Bytes()...)); err != nil {
		log.Crit("Failed to delete tile", "error", err)
	}
}

func WriteStateRoot(db ethdb.KeyValueWriter, hash common.Hash) {
	if err := db.Put(headStateKey, hash.Bytes()); err != nil {
		log.Crit("Failed to write state root", "error", err)
	}
}

func ReadStateRoot(db ethdb.KeyValueReader) (common.Hash, error) {
	blob, err := db.Get(headStateKey)
	if err != nil {
		return common.Hash{}, err
	}
	return common.BytesToHash(blob), nil
}

func DeleteStateRoot(db ethdb.KeyValueWriter) {
	if err := db.Delete(headStateKey); err != nil {
		log.Crit("Failed to delete state root", "error", err)
	}
}
