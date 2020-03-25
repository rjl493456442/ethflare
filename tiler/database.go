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
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/log"
	"github.com/rjl493456442/ethflare/database"
	"github.com/rjl493456442/ethflare/types"
)

// tile represents the metadata of a chunk of state trie.
// It's an inner version with two additional fields.
type tile struct {
	types.Tile

	flushPrev common.Hash // Previous node in the flush-list
	flushNext common.Hash // Next node in the flush-list
}

// diffLayer is memory layer which contains all changes compared with it's
// parent. If the parent layer is nil, which actually reference the disk.
type diffLayer struct {
	parent *diffLayer            // Pointer to parent layer, nil if it's the deepest
	state  common.Hash           // Empty means the tile crawling is not completed yet
	tiles  map[common.Hash]*tile // Tile set, linked by the insertion order
	size   common.StorageSize    // Total node size of maintained tiles

	oldest common.Hash // Oldest tracked node, flush-list head
	newest common.Hash // Newest tracked node, flush-list tail
}

// flush commits the deepest diff layer into database, returns the total
// committed node number or any error occurs.
func (diff *diffLayer) flush(db *tileDatabase) (*diffLayer, int, error) {
	if diff.parent != nil {
		parent, committed, err := diff.parent.flush(db)
		if err != nil {
			return nil, 0, err
		}
		diff.parent = parent // may be set to nil if parent is flushed
		return diff, committed, nil
	}
	batch := db.db.NewBatch()
	for h, tile := range diff.tiles {
		database.WriteTile(batch, h, &tile.Tile)
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return nil, 0, err
			}
			batch.Reset()
		}
	}
	// Commit state root to represent the whole state is tiled.
	if diff.state != (common.Hash{}) {
		database.WriteStateRoot(batch, diff.state)
	}
	if err := batch.Write(); err != nil {
		return nil, 0, err
	}
	log.Trace("Flush layer", "state", diff.state, "number", len(diff.tiles))

	// May be noop since only completed layer will be registered into set
	delete(db.diffset, diff.state)

	// Release memory allocation
	count := len(diff.tiles)
	diff.tiles = nil
	diff.state = common.Hash{}
	diff.oldest, diff.newest = common.Hash{}, common.Hash{}
	return nil, count, nil
}

// cap flushes old tiles into disk until the total tile number maintained below
// a reasonable limit.
//
// If the flushed layer is uncompleted, no relevant state indicator will be committed.
// Otherwise if all nodes are flushed, the state indicator is also committed.
//
// TODO(rjl493456442) Some tiles maintained in the diff layer may not referenced
// by the latest state anymore. Figure out the concrete GC algo.
func (diff *diffLayer) cap(db *tileDatabase, needDrop int) (*diffLayer, int, error) {
	var dropped int
	if diff.parent != nil {
		parent, committed, err := diff.parent.cap(db, needDrop)
		if err != nil {
			return nil, 0, err
		}
		diff.parent = parent // may be set to nil if parent is flushed
		if needDrop <= committed {
			return diff, committed, nil
		}
		needDrop -= committed
		dropped = committed
	}
	// Keep committing nodes from the flush-list until we're below allowance
	batch := db.db.NewBatch()
	oldest := diff.oldest
	for needDrop > 0 && oldest != (common.Hash{}) {
		tile := diff.tiles[oldest]
		database.WriteTile(batch, oldest, &diff.tiles[oldest].Tile)
		if batch.ValueSize() > ethdb.IdealBatchSize {
			if err := batch.Write(); err != nil {
				return nil, 0, err
			}
			batch.Reset()
		}
		oldest = tile.flushNext
		needDrop -= 1
	}
	// Commit state root to represent the whole state is tiled.
	if diff.state != (common.Hash{}) && oldest == (common.Hash{}) {
		database.WriteStateRoot(batch, diff.state)
	}
	// Write all accumulated changes into disk
	if err := batch.Write(); err != nil {
		return nil, 0, err
	}
	if oldest == (common.Hash{}) {
		delete(db.diffset, diff.state) // may be noop if the layer is uncompleted
		count := len(diff.tiles)
		diff.state, diff.oldest = common.Hash{}, common.Hash{}
		diff.tiles = make(map[common.Hash]*tile)
		diff.oldest, diff.newest = common.Hash{}, common.Hash{}
		return nil, dropped + count, nil
	} else {
		var count int
		for diff.oldest != oldest {
			tile := diff.tiles[diff.oldest]
			delete(diff.tiles, diff.oldest)
			diff.oldest = tile.flushNext
			count += 1
		}
		return diff, dropped + count, nil
	}
}

// has return the indicator whether the tile is maintained.
func (diff *diffLayer) has(hash common.Hash) bool {
	t, _ := diff.get(hash)
	return t != nil
}

// get returns the tile metadata if it's maintained.
// The returned state flag may be empty if the layer
// is uncompleted.
func (diff *diffLayer) get(hash common.Hash) (*types.Tile, common.Hash) {
	// Search in this layer first.
	if tile := diff.tiles[hash]; tile != nil {
		return &tile.Tile, diff.state
	}
	// Not found, search in parent layer
	if diff.parent != nil {
		return diff.parent.get(hash)
	}
	// Not found in parent's, return false
	return nil, common.Hash{}
}

const (
	maxTilesThreshold  = 4096 // The maximum tiles can be maintained in memory
	maxLayersThreshold = 12   // The maximum layers can be maintained in memory
)

// tileDatabase is the tile metadata database which splits all tiles
// into different layers. Each layer contains all changes since it's
// parent layer(new tiles generated during one **or many** state transition.
// Whenever the layer is completed(all new tiles are crawled) then it
// will be put into diffset and wait flushing.
//
// If there are too many tiles accumulated in the memory(e.g. initial
// crawling), the tiles will be committed based on the insertion order
// even not all tiles are crawled.
//
// todo(rjl493456442) add bloom filter
type tileDatabase struct {
	db      ethdb.Database
	current *diffLayer

	// todo(rjl493456442) if two layers have same state root,
	// the first one will be replaced silently
	diffset    map[common.Hash]*diffLayer // diff layer set identified by relevant state root
	totalTiles int                        // The counter of all tiles in the memory
	latest     common.Hash                // Pointer to latest registered layer
}

func newTileDatabase(db ethdb.Database) *tileDatabase {
	return &tileDatabase{
		db:      db,
		diffset: make(map[common.Hash]*diffLayer),
	}
}

func (db *tileDatabase) insert(hash common.Hash, depth uint8, number uint16, size common.StorageSize, refs []common.Hash) error {
	// Allocate a new diff layer if necessary
	if db.current == nil {
		var parent *diffLayer
		if db.latest != (common.Hash{}) {
			parent = db.diffset[db.latest]
		}
		db.current = &diffLayer{parent: parent, tiles: make(map[common.Hash]*tile)}
	}
	// Insert the new tile to the set
	db.current.tiles[hash] = &tile{
		Tile: types.Tile{
			Depth: depth,
			Nodes: number,
			Refs:  refs,
		},
	}
	db.current.size += size
	log.Trace("Inserted tile", "hash", hash, "size", size)

	// Update the flush-list endpoints
	if db.current.oldest == (common.Hash{}) {
		db.current.oldest, db.current.newest = hash, hash
	} else {
		db.current.tiles[db.current.newest].flushNext, db.current.newest = hash, hash
	}
	// If we exceeded our memory allowance, flush oldest tile metadata to disk
	db.totalTiles += 1
	if db.totalTiles > maxTilesThreshold {
		start := time.Now()
		diff, committed, err := db.current.cap(db, maxTilesThreshold/2)
		if err != nil {
			return err
		}
		db.current = diff
		db.totalTiles -= committed
		log.Info("Committed tiles", "number", committed, "elapsed", common.PrettyDuration(time.Since(start)))
	}
	return nil
}

// commit marks the current layer as completed, flush deepest layer
// if there are too many accumulated.
func (db *tileDatabase) commit(root common.Hash) error {
	// Short circuit if no tiles need to commit. It can
	// happen like when state transition between tw blocks
	// is empty. e.g. empty blocks created by clique engine.
	if db.current == nil {
		return nil
	}
	// Debug panic, too see how many duplicated state
	if l, ok := db.diffset[root]; ok {
		msg := fmt.Sprintf("old:%d, new:%d", len(l.tiles), len(db.current.tiles))
		panic(msg)
	}
	tiles, size := len(db.current.tiles), db.current.size
	db.current.state = root
	db.diffset[root] = db.current
	db.latest = root
	db.current = nil
	log.Info("Committed state", "root", root, "tiles", tiles, "size", size)

	// Flush the deepest layer into disk if necessary
	if len(db.diffset) > maxLayersThreshold {
		latest := db.diffset[db.latest]
		_, committed, err := latest.flush(db)
		if err != nil {
			return err
		}
		db.totalTiles -= committed
	}
	return nil
}

// close writes all in-memory data into disk
func (db *tileDatabase) close() error {
	for len(db.diffset) > 0 {
		latest := db.diffset[db.latest]
		_, _, err := latest.flush(db)
		if err != nil {
			return err
		}
	}
	if db.current != nil {
		db.current.parent = nil
		_, _, err := db.current.flush(db)
		if err != nil {
			return err
		}
	}
	return nil
}

// has returns the indicator whether the tile is available in database.
func (db *tileDatabase) has(hash common.Hash) bool {
	layer := db.current
	if layer == nil && db.latest != (common.Hash{}) {
		layer = db.diffset[db.latest]
	}
	// Search it in all in-memory layers first
	if layer != nil && layer.has(hash) {
		return true
	}
	// Search it in database later
	t, _ := database.ReadTile(db.db, hash)
	return t != nil
}

// has returns the indicator whether the tile is available in database.
func (db *tileDatabase) get(hash common.Hash) (*types.Tile, common.Hash) {
	layer := db.current
	if layer == nil && db.latest != (common.Hash{}) {
		layer = db.diffset[db.latest]
	}
	// Search it in all in-memory layers first
	if layer != nil {
		if tile, state := layer.get(hash); tile != nil {
			return tile, state // state can be empty if it's uncompleted
		}
	}
	// Search it in database later
	t, _ := database.ReadTile(db.db, hash)
	if t != nil {
		state, _ := database.ReadStateRoot(db.db)
		return &types.Tile{
			Depth: t.Depth,
			Nodes: t.Nodes,
			Refs:  t.Refs,
		}, state
	}
	return nil, common.Hash{} // Not found
}
