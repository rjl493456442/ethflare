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
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/trie"
	"github.com/rjl493456442/ethflare/params"
	"github.com/rjl493456442/ethflare/types"
)

// emptyRoot is the known root hash of an empty trie.
var emptyRoot = common.HexToHash("56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421")

// leafCallback is a callback type invoked when a trie operation reaches a leaf
// node. It's used by state sync and commit to allow handling external references
// between account and storage tries.
type leafCallback func(leaf []byte, parent common.Hash) error

// tileRequest contains all context info of a tile request.
type tileRequest struct {
	hash     common.Hash         // Node hash of tile
	depth    uint8               // Depth tracker to allow reporting per-depth statistics
	parents  []*tileRequest      // Parent tile requests referencing this entry (notify all upon completion)
	deps     int                 // The number of children tiles referenced by it
	attempts map[string]struct{} // The records for already tried nodes
	onLeaf   leafCallback        // Callback applied on leave nodes to expand more tasks
}

// tileDelivery is a tile creation event tagged with the origin backend.
type tileDelivery struct {
	origin string        // backend that should be marked idle on delivery
	hash   common.Hash   // Trie node (tile root) to reschedule upon failure
	nodes  [][]byte      // Delivered tile data upon success
	refs   []common.Hash // Tile references, derived later
	err    error         // Encountered error upon failure
}

// generator is responsible for keeping track of pending state-tile crawl jobs
// and distributing them to backends that have the dataset available.
type generator struct {
	state      common.Hash
	requests   map[common.Hash]*tileRequest
	deliveries map[common.Hash]*tileDelivery
	externRefs map[common.Hash]map[common.Hash]int
	queue      *prque.Prque
	database   *tileDatabase
	stat       *statistic
}

// newGenerator creates a state-tile task generator.
func newGenerator(state common.Hash, database *tileDatabase, stat *statistic) *generator {
	g := &generator{
		state:      state,
		requests:   make(map[common.Hash]*tileRequest),
		deliveries: make(map[common.Hash]*tileDelivery),
		externRefs: make(map[common.Hash]map[common.Hash]int),
		database:   database,
		queue:      prque.New(nil),
		stat:       stat,
	}
	log.Debug("New generator", "state", state)
	return g
}

// hasTile returns indicator whether a specified tile is crawled.
func (g *generator) hasTile(hash common.Hash) bool {
	if g.deliveries[hash] != nil {
		return true
	}
	return g.database.has(hash) // It's quite IO expensive, bloom filter can help it a bit
}

// getTile returns the tile if it's already crawled(may or may not committed)
// and relevant state root. Note the returned state root may actually relevant
// with tile. E.g. in the disk layer all tiles are identified with the latest
// state root.
// So if it's not matched, then the version of state root mush newer than tile
// and the tile may not be referenced by the state anymore.
func (g *generator) getTile(hash common.Hash) (*types.Tile, common.Hash) {
	// Search in memory cache first
	if t := g.deliveries[hash]; t != nil {
		req := g.requests[hash]
		return &types.Tile{
			Depth: req.depth,
			Nodes: uint16(len(t.nodes)),
			Refs:  t.refs,
		}, g.state // It's in memory, return target state hash
	}
	// Then search it in the database
	tile, state := g.database.get(hash)

	// If no associated state is returned, use the latest.
	// The crawled tiles in database may not completed. Assume
	// it's still referenced by latest state. But even if the
	// state is not referenced anymore, it's also okay.
	if tile != nil && state == (common.Hash{}) {
		state = g.state
	}
	return tile, state
}

// addTask puts a new crawling task into the task queue.
func (g *generator) addTask(hash common.Hash, depth uint8, parent common.Hash, onLeaf leafCallback) {
	// Short circuit if the tile is empty, usually it
	// can happen to add a storage task while storage
	// is empty.
	if hash == emptyRoot {
		atomic.AddUint32(&g.stat.emptyTask, 1)
		log.Debug("Empty task", "hash", hash, "parent", parent, "depth", depth, "storage", onLeaf == nil)
		return
	}
	// Short circuit if the tile is already known
	if g.hasTile(hash) {
		atomic.AddUint32(&g.stat.duplicateTask, 1)
		log.Debug("Duplicated task", "hash", hash, "parent", parent, "depth", depth, "storage", onLeaf == nil)
		return
	}
	// Assemble the new sub-tile sync request
	req := &tileRequest{
		hash:     hash,
		depth:    depth,
		attempts: make(map[string]struct{}),
		onLeaf:   onLeaf,
	}
	// If this sub-trie has a designated parent, link them together
	if parent != (common.Hash{}) {
		ancestor := g.requests[parent]
		if ancestor == nil {
			panic(fmt.Sprintf("sub-tile ancestor not found: %x", parent))
		}
		ancestor.deps++
		req.parents = append(req.parents, ancestor)
	}
	g.schedule(req)
	log.Debug("Add task", "hash", hash, "parent", parent, "depth", depth, "storage", onLeaf == nil)
}

// reference adds an external reference for state tile and storage tile.
func (g *generator) reference(parent, child common.Hash) {
	if child == emptyRoot {
		return
	}
	// If it's already referenced, bump the reference count
	// It can happen the parent tile includes account A, B, C
	// While the root storage tile of A, B, C can be same(e.g.
	// factory contract)
	if _, ok := g.externRefs[parent]; !ok {
		g.externRefs[parent] = make(map[common.Hash]int)
	}
	g.externRefs[parent][child] += 1
	log.Trace("Reference externally", "parent", parent, "child", child)
}

// schedule inserts a new tile retrieval request into the fetch queue. If there
// is already a pending request for this node, the new request will be discarded
// and only a parent reference added to the old one.
func (g *generator) schedule(req *tileRequest) {
	// If we're already requesting this node, add a new reference and stop.
	if old, ok := g.requests[req.hash]; ok {
		atomic.AddUint32(&g.stat.mergedTask, 1)
		old.parents = append(old.parents, req.parents...)
		return
	}
	// Schedule the request for future retrieval
	g.queue.Push(req.hash, int64(req.depth))
	g.requests[req.hash] = req

	if req.onLeaf != nil {
		atomic.AddUint32(&g.stat.stateTask, 1)
	} else {
		atomic.AddUint32(&g.stat.storageTask, 1)
	}
}

// assignTasks pops a task from queue which is not sent
// to given node.
func (g *generator) assignTasks(nodeid string) common.Hash {
	var (
		depths  []uint8
		poplist []common.Hash
	)
	defer func() {
		for i := 0; i < len(poplist); i++ {
			g.queue.Push(poplist[i], int64(depths[i]))
		}
	}()
	for !g.queue.Empty() {
		hash := g.queue.PopItem().(common.Hash)
		request := g.requests[hash]
		if _, ok := request.attempts[nodeid]; ok {
			poplist, depths = append(poplist, hash), append(depths, request.depth)
			continue
		}
		request.attempts[nodeid] = struct{}{}
		if hash == (common.Hash{}) {
			panic("Empty task")
		}
		return hash
	}
	return common.Hash{} // No more task available
}

// pending returns the number of inflight requests
func (g *generator) pending() int {
	return len(g.requests)
}

// forAllPending iterates all pending requests(mainly for debugging purpose)
func (g *generator) forAllPending(callback func(req *tileRequest) bool) {
	var (
		depths  []uint8
		poplist []common.Hash
	)
	defer func() {
		for i := 0; i < len(poplist); i++ {
			g.queue.Push(poplist[i], int64(depths[i]))
		}
	}()
	for !g.queue.Empty() {
		hash := g.queue.PopItem().(common.Hash)
		request := g.requests[hash]
		poplist, depths = append(poplist, hash), append(depths, request.depth)

		if !callback(request) {
			return
		}
	}
}

// process injects the retrieved tile and expands more sub tasks from the
// tile references.
func (g *generator) process(delivery *tileDelivery, nodes []string) error {
	if g.requests[delivery.hash] == nil {
		atomic.AddUint32(&g.stat.unsolicitedReply, 1)
		return errors.New("non-existent request")
	}
	atomic.AddUint32(&g.stat.deliveries, 1)

	// If tile retrieval failed or nothing returned, reschedule it
	request := g.requests[delivery.hash]
	if delivery.err != nil || len(delivery.nodes) == 0 {
		// Check whether there still exists some available nodes to retry.
		// It might exists some data race that some nodes is removed after
		// we re-push the task which is the only one available for task.
		//
		// It's ok since the whole generator will be created after several
		// blocks.
		//
		// todo(rjl493456442) add a new mechanism to drop all "dead tasks"
		// when we drop some nodes.
		for _, n := range nodes {
			if _, ok := request.attempts[n]; !ok {
				g.queue.Push(request.hash, int64(request.depth))
				return nil
			}
		}
		// If we already try all nodes to fetch it, discard it
		// silently. It's ok if the tile is still referenced by state,
		// we can retrieve it later.
		atomic.AddUint32(&g.stat.failures, 1)
		delete(g.requests, delivery.hash)
		return nil
	}
	// Eliminate the intermediate nodes and their children if they are already tiled.
	var i int
	var removedRefs = make(map[common.Hash]bool)
	hashes := make(map[common.Hash]struct{})
	for index, node := range delivery.nodes {
		hash := crypto.Keccak256Hash(node)
		if g.hasTile(hash) || removedRefs[hash] {
			// If the first node is also eliminated, it means the whole tile
			// is retrieved by other means, discard the whole response.
			//
			// todo why the task will be created in the first place?
			if index == 0 {
				atomic.AddUint32(&g.stat.dropEntireTile, 1)
				delete(g.requests, request.hash)
				_ = g.commitParent(request)
				return nil
			}
			_ = trie.IterateRefs(node, func(path []byte, child common.Hash) error {
				removedRefs[child] = true
				return nil
			}, nil)
			continue
		}
		delivery.nodes[i] = node
		hashes[hash] = struct{}{}
		i++
	}
	if len(delivery.nodes) >= i+1 {
		atomic.AddUint32(&g.stat.dropPartialTile, 1)
	}
	delivery.nodes = delivery.nodes[:i]

	// Expand more children tasks, the first node should never be eliminated
	depths := map[common.Hash]uint8{
		crypto.Keccak256Hash(delivery.nodes[0]): request.depth,
	}
	var children []*tileRequest
	for _, node := range delivery.nodes {
		_ = trie.IterateRefs(node, func(path []byte, child common.Hash) error {
			depths[child] = depths[crypto.Keccak256Hash(node)] + uint8(len(path))
			if _, ok := hashes[child]; !ok {
				delivery.refs = append(delivery.refs, child)

				// Add the ref as the task if it's still not crawled.
				if !g.hasTile(child) {
					children = append(children, &tileRequest{
						hash:     child,
						depth:    depths[child],
						parents:  []*tileRequest{request},
						attempts: make(map[string]struct{}),
						onLeaf:   request.onLeaf,
					})
				}
			}
			return nil
		}, func(path []byte, node []byte) error {
			if request.onLeaf != nil {
				_ = request.onLeaf(node, request.hash)
			}
			return nil
		})
	}
	log.Trace("Delivered tile", "hash", request.hash, "nodes", len(delivery.nodes), "reference", len(delivery.refs))

	// We still need to check whether deps is zero or not.
	// Sub task may be created via callback.
	if len(children) == 0 && request.deps == 0 {
		_ = g.commit(request, delivery)
		return nil
	}
	request.deps += len(children)
	g.deliveries[request.hash] = delivery

	for _, child := range children {
		g.schedule(child)
	}
	return nil
}

// commit finalizes a retrieval request and stores it into the membatch. If any
// of the referencing parent requests complete due to this commit, they are also
// committed themselves.
func (g *generator) commit(req *tileRequest, delivery *tileDelivery) error {
	// If the tile is too small, merge it to parent
	if len(delivery.nodes) < params.TileMinimalSize && len(req.parents) > 0 {
		for _, p := range req.parents {
			parent := g.deliveries[p.hash]
			parent.nodes = append(parent.nodes, delivery.nodes...)
			// Wipe the reference in parent. There are two spaces can contain the ref:
			// 1) the parent's delivery packet
			// 2) the external map
			// If nothing found, panic is expected.
			var removed bool
			for index, ref := range parent.refs {
				if ref == req.hash {
					removed = true
					parent.refs = append(parent.refs[:index], parent.refs[index+1:]...)
					break
				}
			}
			if !removed {
				refs := g.externRefs[p.hash]
				if refs != nil {
					refs[req.hash] -= 1
					if refs[req.hash] == 0 {
						delete(g.externRefs[p.hash], req.hash)
					}
					removed = true
				}
			}
			if !removed {
				panic("no reference found")
			}
			parent.refs = append(parent.refs, delivery.refs...)
		}
		delete(g.deliveries, req.hash)
		delete(g.requests, req.hash)
		atomic.AddUint32(&g.stat.mergeTile, 1)
		return g.commitParent(req)
	}
	// Inject itself to database.
	var storage common.StorageSize
	for _, node := range delivery.nodes {
		storage += common.StorageSize(len(node))
	}
	// If the committed tile is a leaf of state trie,
	// add the additional external reference.
	if children, ok := g.externRefs[req.hash]; ok {
		for hash := range children {
			delivery.refs = append(delivery.refs, hash)
		}
		delete(g.externRefs, req.hash)
	}
	if err := g.database.insert(req.hash, req.depth, uint16(len(delivery.nodes)), storage, delivery.refs); err != nil {
		return err
	}
	delete(g.deliveries, req.hash)
	delete(g.requests, req.hash)

	// Check all parents for completion
	return g.commitParent(req)
}

// commitParent recursively commits parent cached delivery if no dependency.
func (g *generator) commitParent(req *tileRequest) error {
	for _, parent := range req.parents {
		parent.deps--
		if parent.deps == 0 {
			if err := g.commit(parent, g.deliveries[parent.hash]); err != nil {
				return err
			}
		}
	}
	return nil
}
