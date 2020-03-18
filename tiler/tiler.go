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
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/state"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/rjl493456442/ethflare/params"
	etype "github.com/rjl493456442/ethflare/types"
)

// Node is the interface of backend node
type Node interface {
	ID() string
	GetTile(ctx context.Context, hash common.Hash) ([][]byte, error)
	SubscribeNewHead(sink chan *types.Header) event.Subscription
	Logger() log.Logger
}

// headAnnounce is a new-head event tagged with the origin backend.
type headAnnounce struct {
	origin Node
	head   *types.Header
}

// statistic is a set of stats for logging.
type statistic struct {
	storageTask      uint32 // Total number of allocated storage trie task
	stateTask        uint32 // Total number of allocated state trie task
	dropEntireTile   uint32 // Counter for dropping entire fetched tile to dedup
	dropPartialTile  uint32 // Counter for dropping partial fetched tile
	mergeTile        uint32 // Counter for small tile be merged
	duplicateTask    uint32 // Counter of allocated but duplicated task
	emptyTask        uint32 // Counter of allocated but empty task
	failures         uint32 // Counter of tile task failures
	unsolicitedReply uint32 // Counter of unsolicited response
}

// tileQuery represents a request for asking whether a specified
// tile is indexed.
type tileQuery struct {
	hash   common.Hash
	result chan tileAnswer
}

// tileAnswer represents an answer for tile querying.
type tileAnswer struct {
	tile  *etype.Tile
	state common.Hash
}

// Tiler is responsible for keeping track of pending state and chain crawl
// jobs and distributing them to backends that have the dataset available.
type Tiler struct {
	db          ethdb.Database
	addNodeCh   chan Node
	remNodeCh   chan Node
	newHeadCh   chan *headAnnounce
	newTileCh   chan *tileDelivery
	tileQueryCh chan *tileQuery
	closeCh     chan struct{}
	removeNode  func(string) error
	stat        *statistic
	wg          sync.WaitGroup
}

// NewTiler creates a tile crawl Tiler.
func NewTiler(db ethdb.Database, removeNode func(string) error) *Tiler {
	s := &Tiler{
		db:          db,
		addNodeCh:   make(chan Node),
		remNodeCh:   make(chan Node),
		newHeadCh:   make(chan *headAnnounce),
		newTileCh:   make(chan *tileDelivery),
		tileQueryCh: make(chan *tileQuery),
		removeNode:  removeNode,
		closeCh:     make(chan struct{}),
		stat:        &statistic{},
	}
	return s
}

func (t *Tiler) Start() {
	t.wg.Add(2)
	go t.loop()
	go t.logger()
}

func (t *Tiler) Stop() {
	close(t.closeCh)
	t.wg.Wait()
}

// loop is the main event loop of the Tiler, receiving various events and
// acting on them.
func (t *Tiler) loop() {
	defer t.wg.Done()

	var (
		ids      []string
		nodes    = make(map[string]Node)
		heads    = make(map[string]*types.Header)
		assigned = make(map[string]struct{})

		generator *generator
		pivot     *types.Header
		latest    *types.Header
		tileDB    = newTileDatabase(t.db)
	)

	for {
		// Assign new tasks to nodes for tile indexing
		if generator != nil {
			for id, node := range nodes {
				if _, ok := assigned[id]; ok {
					continue
				}
				task := generator.assignTasks(id)
				if task == (common.Hash{}) {
					continue
				}
				assigned[id] = struct{}{}

				// Better solution, using a worker pool instead
				// of creating routines infinitely.
				go func() {
					ctx, cancelFn := context.WithTimeout(context.Background(), time.Second*10)
					defer cancelFn()

					nodes, err := node.GetTile(ctx, task)
					t.newTileCh <- &tileDelivery{
						origin: node.ID(),
						hash:   task,
						nodes:  nodes,
						err:    err,
					}
				}()
			}
			if generator.pending() == 0 && pivot != nil {
				if err := tileDB.commit(pivot.Root); err != nil {
					log.Error("Failed to commit state", "error", err)
				} else {
					log.Info("Tiles indexed", "number", pivot.Number, "hash", pivot.Hash(), "state", pivot.Root)
					pivot = nil
				}
			}
		}
		// Handle any events, not much to do if nothing happens
		select {
		case <-t.closeCh:
			return

		case b := <-t.addNodeCh:
			if _, ok := nodes[b.ID()]; ok {
				b.Logger().Error("node already registered in tiler")
				continue
			}
			b.Logger().Info("node registered into tiler")
			go t.muxHeadEvents(b)
			nodes[b.ID()] = b
			ids = append(ids, b.ID())

		case b := <-t.remNodeCh:
			if _, ok := nodes[b.ID()]; !ok {
				b.Logger().Error("node not registered in tiler")
				continue
			}
			b.Logger().Info("node unregistered from tiler")
			delete(nodes, b.ID())

			for index, id := range ids {
				if id == b.ID() {
					ids = append(ids[:index], ids[index+1:]...)
					break
				}
			}
			if t.removeNode != nil {
				t.removeNode(b.ID())
			}

		case announce := <-t.newHeadCh:
			heads[announce.origin.ID()] = announce.head
			if latest == nil || announce.head.Number.Uint64() > latest.Number.Uint64() {
				latest = announce.head
			}
			callback := func(leaf []byte, parent common.Hash) error {
				var obj state.Account
				if err := rlp.Decode(bytes.NewReader(leaf), &obj); err != nil {
					return err
				}
				generator.addTask(obj.Root, 64, parent, nil) // Spin up more sub tasks for storage trie
				return nil
			}
			// If it's first fired or newer state is available, create generator to generate crawl tasks
			//
			// In the best situation, all different version state will be indexed. However if the tiler
			// can't finish indexing in a block time(~15s), several state will be merged as a single one.
			if pivot == nil || pivot.Number.Uint64()+params.RecentnessCutoff <= announce.head.Number.Uint64() {
				pivot = announce.head
				generator = newGenerator(pivot.Root, tileDB, t.stat)
				generator.addTask(pivot.Root, 0, common.Hash{}, callback)
			}

		case tile := <-t.newTileCh:
			delete(assigned, tile.origin)
			generator.process(tile, ids)

		case req := <-t.tileQueryCh:
			tile, state := generator.getTile(req.hash)
			req.result <- tileAnswer{
				tile:  tile,
				state: state,
			}
		}
	}
}

// muxHeadEvents registers a watcher for new chain head events on the backend and
// multiplexes the events into a common channel for the Tiler to handle.
func (t *Tiler) muxHeadEvents(n Node) {
	heads := make(chan *types.Header)

	sub := n.SubscribeNewHead(heads)
	defer sub.Unsubscribe()

	for {
		select {
		case head := <-heads:
			n.Logger().Trace("new header", "hash", head.Hash(), "number", head.Number)
			t.newHeadCh <- &headAnnounce{origin: n, head: head}
		case err := <-sub.Err():
			n.Logger().Warn("backend head subscription failed", "err", err)
			t.remNodeCh <- n
			return
		}
	}
}

// RegisterNode starts tracking a new Ethereum backend to delegate tasks to.
func (t *Tiler) RegisterNode(n Node) {
	select {
	case t.addNodeCh <- n:
	case <-t.closeCh:
	}
}

func (t *Tiler) GetTile(root common.Hash) (*etype.Tile, common.Hash) {
	result := make(chan tileAnswer, 1)
	select {
	case t.tileQueryCh <- &tileQuery{
		hash:   root,
		result: result,
	}:
		answer := <-result
		return answer.tile, answer.state
	case <-t.closeCh:
		return nil, common.Hash{}
	}
}

// logger is a helper loop to print internal statistic with a fixed time interval.
func (t *Tiler) logger() {
	defer t.wg.Done()

	ticker := time.NewTicker(time.Second * 10)
	defer ticker.Stop()

	for {
		select {
		case <-t.closeCh:
			return
		case <-ticker.C:
			log.Info("Tiler progress", "state", atomic.LoadUint32(&t.stat.stateTask), "storage", atomic.LoadUint32(&t.stat.storageTask),
				"drop", atomic.LoadUint32(&t.stat.dropEntireTile), "droppart", atomic.LoadUint32(&t.stat.dropPartialTile),
				"merge", atomic.LoadUint32(&t.stat.mergeTile), "duplicate", atomic.LoadUint32(&t.stat.duplicateTask),
				"empty", atomic.LoadUint32(&t.stat.emptyTask), "failure", atomic.LoadUint32(&t.stat.failures),
				"unsolicited", atomic.LoadUint32(&t.stat.unsolicitedReply))
		}
	}
}
