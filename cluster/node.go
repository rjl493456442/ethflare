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

package cluster

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/event"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rjl493456442/ethflare/params"
	"golang.org/x/time/rate"
)

// Node represents an Ethereum node with the ethflare RPC API implemented and
// exposed via WebSockets (we need notification support for new heads).
//
// The necessary ethflare APIs includes:
//   - cdn_tile
//   - cdn_nodes
//   - cdn_receipts
type Node struct {
	id     string
	conn   *rpc.Client
	client *ethclient.Client

	headSub  ethereum.Subscription
	headCh   chan *types.Header
	headFeed *event.Feed

	// Status of Node
	headers map[common.Hash]*types.Header // Set of recent headers across all mini-forks
	recents *prque.Prque                  // Priority queue for evicting stale headers
	states  map[common.Hash]int           // Set of state roots available for tiling
	limiter *rate.Limiter                 // Rate limit to provent adding too much pressure

	logger  log.Logger
	lock    sync.RWMutex
	closeCh chan struct{}
}

// NewNode takes a live websocket connection to a Node and starts to monitor
// its chain progression and optionally request chain and state data.
func NewNode(id string, conn *rpc.Client, ratelimit uint64) (*Node, error) {
	client := ethclient.NewClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	sink := make(chan *types.Header, 256)
	sub, err := client.SubscribeNewHead(ctx, sink)
	if err != nil {
		return nil, err
	}
	node := &Node{
		id:       id,
		conn:     conn,
		client:   client,
		headSub:  sub,
		headCh:   sink,
		headFeed: new(event.Feed),
		headers:  make(map[common.Hash]*types.Header),
		recents:  prque.New(nil),
		states:   make(map[common.Hash]int),
		limiter:  rate.NewLimiter(rate.Limit(ratelimit), 10),
		logger:   log.New("id", id),
		closeCh:  make(chan struct{}),
	}
	return node, nil
}

func (n *Node) start() {
	go n.loop()
}

func (n *Node) stop() {
	close(n.closeCh)
	n.headSub.Unsubscribe()
}

// loop keeps exhausting the head header announcement channel, maintaining the
// current fork tree as seen by the backing node.
func (n *Node) loop() {
	var updating chan struct{}

	for {
		select {
		case head := <-n.headCh:
			// New head announced, update the fork tree if we're not currently updating
			if updating == nil {
				updating = make(chan struct{})
				go func() {
					if err := n.update(head); err != nil {
						n.logger.Warn("Failed to update to new head", "err", err)
					} else {
						n.logger.Debug("Updated to new head", "number", head.Number, "hash", head.Hash(), "root", head.Root)
						n.headFeed.Send(head)
					}
					updating <- struct{}{}
				}()
			}

		case <-updating:
			updating = nil

		case <-n.headSub.Err():
			// Subscription died, terminate the loop
			return

		case <-n.closeCh:
			// Backend is closed, terminate the loop
			return
		}
	}
}

// update extends the currently maintained fork tree of this backing node with a
// new head and it's progression towards already known blocks.
func (n *Node) update(head *types.Header) error {
	n.lock.Lock()
	defer n.lock.Unlock()

	// If the Node is fresh new, track the header here.
	if len(n.headers) == 0 {
		n.updateAll(head, 0, true)
		return nil
	}
	// Already track a batch of headers, ensure they are still valid.
	if err := n.updateAll(head, 0, false); err != nil {
		n.reset()
		n.updateAll(head, 0, true)
	}
	return nil
}

func (n *Node) reset() {
	n.headers = make(map[common.Hash]*types.Header)
	n.recents = prque.New(nil)
	n.states = make(map[common.Hash]int)
}

// updateAll is the internal version update which assumes the lock is already held.
// updateAll recursively updates all parent information of given header, terminates
// if the depth is too high.
func (n *Node) updateAll(head *types.Header, depth int, init bool) error {
	// If the header is known already known, bail out
	hash := head.Hash()
	if _, ok := n.headers[hash]; ok {
		return nil
	}
	// If the parent lookup reached the limit without hitting a recently announced
	// head, the entire parent chain needs to be discarded since there's no way to
	// know if associated state is present or not (node reboot)
	if depth >= params.RecentnessCutoff {
		if !init {
			return errors.New("exceeded recentness custoff threshold")
		}
		return nil
	}
	// Otherwise track all parents first, then the new head
	if head.Number.Uint64() > 0 {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		parent, err := n.client.HeaderByHash(ctx, head.ParentHash)
		if err != nil {
			return err
		}
		if err := n.updateAll(parent, depth+1, init); err != nil {
			n.logger.Warn("Rejecting uncertain state block", "number", parent.Number, "hash", parent.Hash(), "err", err)
			if depth > 0 { // Head is surely available, it was just announced
				return errors.New("couldn't prove state availability")
			}
		}
	}
	// All parents tracked, add the new head too
	n.logger.Debug("Tracking new block", "number", head.Number, "hash", hash)
	n.recents.Push(hash, -head.Number.Int64())
	n.headers[hash] = head
	n.states[head.Root]++
	if n.states[head.Root] == 1 {
		n.logger.Debug("Tracking new state", "root", head.Root)
	}
	// Since state is pruned, untrack anything older than the cutoff
	for !n.recents.Empty() {
		if item, prio := n.recents.Peek(); -prio <= head.Number.Int64()-params.RecentnessCutoff {
			var (
				hash   = item.(common.Hash)
				header = n.headers[hash]
			)
			n.logger.Debug("Untracking old block", "number", header.Number, "hash", hash)

			delete(n.headers, hash)
			n.recents.PopItem()

			n.states[header.Root]--
			if n.states[header.Root] == 0 {
				n.logger.Debug("Untracking old state", "root", header.Root)
				delete(n.states, header.Root)
			}
			continue
		}
		break
	}
	return nil
}

// SubscribeNewHead subscribes to new chain head events to act as triggers for
// the task tiler.
func (n *Node) SubscribeNewHead(sink chan *types.Header) event.Subscription {
	return n.headFeed.Subscribe(sink)
}

// HasState checks whether a state is available from this Node.
// If the state is too old, then regard it as unavailable.
func (n *Node) HasState(root common.Hash) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	_, ok := n.states[root]
	return ok
}

// HasBlock checks whether a block is available from this Node.
// If the block is too old, return false also.
func (n *Node) HasBlock(hash common.Hash) bool {
	n.lock.RLock()
	defer n.lock.RUnlock()

	_, ok := n.headers[hash]
	return ok
}

// GetTile sends a RPC request for retrieving specified tile
// with given root hash.
func (n *Node) GetTile(ctx context.Context, hash common.Hash) ([][]byte, error) {
	if err := n.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	n.logger.Trace("Fetching state tile", "hash", hash)

	var result [][]byte
	start := time.Now()
	err := n.conn.CallContext(ctx, &result, "cdn_tile", hash, 16, 256, 2)
	if err != nil {
		n.logger.Trace("Failed to fetch state tile", "hash", hash, "error", err)
	} else {
		n.logger.Trace("State tile fetched", "hash", hash, "nodes", len(result), "elapsed", time.Since(start))
	}
	return result, err
}

// GetNodes sends a RPC request for retrieving specified nodes with
// with given node hash list.
func (n *Node) GetNodes(ctx context.Context, hashes []common.Hash) ([][]byte, error) {
	if err := n.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	n.logger.Trace("Fetching state nodes", "number", len(hashes))

	var result [][]byte
	start := time.Now()
	err := n.conn.CallContext(ctx, &result, "cdn_nodes", hashes)
	if err != nil {
		n.logger.Trace("Failed to fetch state nodes", "error", err)
	} else {
		n.logger.Trace("State nodes fetched", "nodes", len(result), "elapsed", time.Since(start))
	}
	return result, err
}

// GetBlockByHash sends a RPC request for retrieving specified block with
// given hash.
func (n *Node) GetBlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	if err := n.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	n.logger.Trace("Fetching block header", "hash", hash)

	block, err := n.client.BlockByHash(ctx, hash)
	if err != nil {
		return nil, err
	}
	return block, nil
}

// GetReceiptsByHash sends a RPC request for retrieving specified receipts
// with given block hash.
func (n *Node) GetReceiptsByHash(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	if err := n.limiter.Wait(ctx); err != nil {
		return nil, err
	}
	n.logger.Trace("Fetching receipts", "hash", hash)

	var result types.Receipts
	start := time.Now()
	err := n.conn.CallContext(ctx, &result, "cdn_receipts", hash)
	if err != nil {
		n.logger.Trace("Failed to fetch receipts", "error", err)
	} else {
		n.logger.Trace("Receipts fetched", "elapsed", time.Since(start))
	}
	return result, nil
}

// ID returns the identity of node
func (n *Node) ID() string {
	return n.id
}

// ID returns the identity of node
func (n *Node) Logger() log.Logger {
	return n.logger
}

// NodeSet is a set of connected nodes
type NodeSet struct {
	lock sync.RWMutex
	set  map[string]*Node
}

func NewNodeSet() *NodeSet {
	return &NodeSet{set: make(map[string]*Node)}
}

// AddNode adds new Node to set, return error if it's already registered.
func (set *NodeSet) AddNode(id string, n *Node) error {
	set.lock.Lock()
	defer set.lock.Unlock()

	if _, ok := set.set[id]; ok {
		return errors.New("duplicated Node")
	}
	set.set[id] = n
	return nil
}

// RemoveNode removes the node from set, return error if it's non-existent.
func (set *NodeSet) RemoveNode(id string) error {
	set.lock.Lock()
	defer set.lock.Unlock()

	if _, ok := set.set[id]; !ok {
		return errors.New("non-existent node")
	}
	delete(set.set, id)
	return nil
}

// HasState returns a list of suitable nodes which has the specified state.
func (set *NodeSet) HasState(root common.Hash) []*Node {
	set.lock.RLock()
	defer set.lock.RUnlock()

	var nodes []*Node
	for _, node := range set.set {
		if node.HasState(root) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// HasBlock returns a list of suitable nodes which has the specified block.
func (set *NodeSet) HasBlock(hash common.Hash) []*Node {
	set.lock.RLock()
	defer set.lock.RUnlock()

	var nodes []*Node
	for _, node := range set.set {
		if node.HasBlock(hash) {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

// Node returns the node instance by id.
func (set *NodeSet) Node(id string) *Node {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return set.set[id]
}

// ForEach iterates the whole Node set and applies callback on each of them.
// Stop iteration when the callback returns false.
func (set *NodeSet) ForEach(callback func(id string, node *Node) bool) {
	set.lock.RLock()
	defer set.lock.RUnlock()

	if callback == nil {
		return
	}
	for id, node := range set.set {
		if !callback(id, node) {
			return
		}
	}
}

// Random returns a random node based on map iteration
func (set *NodeSet) Random() *Node {
	set.lock.RLock()
	defer set.lock.RUnlock()

	for _, n := range set.set {
		return n
	}
	return nil
}

// Len returns the total node number of set
func (set *NodeSet) Len() int {
	set.lock.RLock()
	defer set.lock.RUnlock()

	return len(set.set)
}
