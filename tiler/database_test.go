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

package tiler

import (
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
)

func createTestLayer(db *tileDatabase, state common.Hash, start int) {
	for i := 0; i < 10; i++ {
		hashes := map[common.Hash]struct{}{
			common.HexToHash("deadbeef"): {},
			common.HexToHash("cafebabe"): {},
		}
		refs := []common.Hash{common.HexToHash("deadbeef"), common.HexToHash("cafebabe")}
		db.insert(common.HexToHash(fmt.Sprintf("%x", start*10+i+1)), uint8(i), common.StorageSize(100), hashes, refs)
	}
	db.commit(state)
}

func createTestLayers(db *tileDatabase, n int) {
	for i := 0; i < n; i++ {
		createTestLayer(db, common.HexToHash(fmt.Sprintf("%x", i+1)), i)
	}
}

func TestFlushLayer(t *testing.T) {
	db := newTileDatabase(rawdb.NewMemoryDatabase())
	createTestLayers(db, 10)

	check := func(tiles, levels int) {
		var tTiles int
		var tLevels int
		current := db.diffset[db.latest]
		for current != nil {
			tTiles += len(current.tiles)
			tLevels += 1
			current = current.parent
		}
		if tTiles != tiles {
			t.Fatalf("Total tiles mismatch, want %d, got %d", tiles, tTiles)
		}
		if tLevels != levels {
			t.Fatalf("Total level mismatch, want %d, got %d", levels, tLevels)
		}
	}
	for i := 0; i < 10; i++ {
		latest := db.diffset[db.latest]
		latest.flush(db)
		check(90-i*10, 9-i)
	}
}

func TestCapLayer(t *testing.T) {
	db := newTileDatabase(rawdb.NewMemoryDatabase())
	createTestLayers(db, 10)

	check := func(tiles int, levels int) {
		var tTiles int
		var tLevels int
		current := db.diffset[db.latest]
		for current != nil {
			tTiles += len(current.tiles)
			tLevels += 1
			current = current.parent
		}
		if tTiles != tiles {
			t.Fatalf("Total tiles mismatch, want %d, got %d", tiles, tTiles)
		}
		if tLevels != levels {
			t.Fatalf("Total level mismatch, want %d, got %d", levels, tLevels)
		}
	}
	// Before do anything, check the layers and tiles
	check(100, 10)

	// Drop 5 tiles in the deepest level
	latest := db.diffset[db.latest]
	latest.cap(db, 5)
	check(95, 10)

	// Drop another 5 tiles, the deepest level should be flushed
	latest.cap(db, 5)
	check(90, 9)

	// Drop all tiles
	latest.cap(db, 90)
	check(0, 0)
}

func TestDatabaseQuery(t *testing.T) {
	db := newTileDatabase(rawdb.NewMemoryDatabase())
	createTestLayers(db, 10)

	check := func(start, end int, existent bool) {
		for i := start; i <= end; i++ {
			has := db.has(common.HexToHash(fmt.Sprintf("%x", i)))
			if existent && !has {
				t.Fatal("Tile should be included")
			}
			if !existent && has {
				t.Fatal("Tile shouldn't be included")
			}
			tile, _ := db.get(common.HexToHash(fmt.Sprintf("%x", i)))
			if existent && tile == nil {
				t.Fatal("Tile should be included")
			}
			if !existent && tile != nil {
				t.Fatal("Tile shouldn't be included")
			}
		}
	}
	// Check existence
	check(1, 100, true)

	// Check nonexistence
	check(101, 120, false)

	// Check existence after flushing
	latest := db.diffset[db.latest]
	latest.flush(db)
	check(1, 100, true)

	// Check existence after capping
	latest.cap(db, 85)
	check(1, 100, true)

	// Check existence after capping all
	latest.cap(db, 5)
	check(1, 100, true)
}
