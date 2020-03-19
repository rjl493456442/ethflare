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
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	mrand "math/rand"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rjl493456442/ethflare/database"
	"github.com/rjl493456442/ethflare/tiler"
)

var (
	ErrInvalidParams   = errors.New("invalid parameters")
	ErrUndefinedMethod = errors.New("undefined method")
	ErrZeroNode        = errors.New("no backend available for requests")
	ErrNotFound        = errors.New("data unavailable")
)

// Cluster represents the whole infrastructure for ethflare. It's responsible
// for following things.
//
// - Create tile indexes
//   The whole ethereum state will be cut into several chunks referenced
//   together with each other. Each chunk is called "tile". Since each
//   Geth node can have totally different state(out of sync, synced, fresh
//   new), so cluster will maintain a global tile indexes based on all
//   underlying Geth nodes. The indexes(metadata) can be used for validating
//   the tile request and for request distribution.
//
// - Serve actual requests
//   All requests received in the httpserver will be processed here including
//   chain requests and state requests.
type Cluster struct {
	config *Config

	// tiler is responsible for indexing state trie and
	// providing state request validation service.
	tiler *tiler.Tiler
	nodes *NodeSet
	rand  *mrand.Rand
	log   log.Logger

	once   sync.Once
	closed chan struct{}
}

// NewCluster returns a cluster instance
func NewCluster(config *Config) (*Cluster, error) {
	db, err := database.NewDatabase(config.Datadir)
	if err != nil {
		return nil, err
	}
	seedb := make([]byte, 8)
	crand.Read(seedb)
	seed := int64(binary.BigEndian.Uint64(seedb))
	rand := mrand.New(mrand.NewSource(seed))

	nodes := NewNodeSet()
	return &Cluster{
		config: config,
		nodes:  nodes,
		log:    config.Log,
		rand:   rand,
		tiler:  tiler.NewTiler(db, nodes.RemoveNode),
		closed: make(chan struct{}),
	}, nil
}

// Start starts the whole cluster, track the latest ethereum state
// and crawl the missing tile.
func (c *Cluster) Start() {
	// Create and start all configured nodes.
	c.tiler.Start()
	for _, n := range c.config.Nodes {
		endpoint := strings.TrimSpace(n.Endpoint)
		conn, err := rpc.Dial(endpoint)
		if err != nil {
			c.log.Warn("Failed to dial remote node", "endpoint", endpoint, "err", err)
			continue
		}
		node, err := NewNode(n.ID, conn, n.RateLimit)
		if err != nil {
			c.log.Debug("Failed to initialize node", "error", err)
			continue
		}
		node.start()
		if err := c.nodes.AddNode(n.ID, node); err != nil {
			c.log.Debug("Failed to register node", "error", err)
			continue
		}
		c.tiler.RegisterNode(node)
	}
}

func (c *Cluster) Stop() {
	c.tiler.Stop()
	c.once.Do(func() {
		close(c.closed)
	})
}

func (c *Cluster) Wait() {
	<-c.closed
}

// ServeRequest performs the request serving with given arguments.
func (c *Cluster) ServeRequest(ctx context.Context, method string, args ...interface{}) (interface{}, error) {
	if c.nodes.Len() == 0 {
		return nil, ErrZeroNode
	}
	switch method {
	case "GetBlockByHash":
		if len(args) != 1 {
			return nil, ErrInvalidParams
		}
		hash, ok := args[0].(common.Hash)
		if !ok {
			return nil, ErrInvalidParams
		}
		return c.getBlockByHash(ctx, hash)

	case "GetReceiptsByHash":
		if len(args) != 1 {
			return nil, ErrInvalidParams
		}
		hash, ok := args[0].(common.Hash)
		if !ok {
			return nil, ErrInvalidParams
		}
		return c.getReceiptsByHash(ctx, hash)

	case "GetNodes":
		if len(args) != 1 {
			return nil, ErrInvalidParams
		}
		root, ok := args[0].(common.Hash)
		if !ok {
			return nil, ErrInvalidParams
		}
		return c.getNodes(ctx, root)

	default:
		return nil, ErrUndefinedMethod
	}
}

func (c *Cluster) getBlockByHash(ctx context.Context, hash common.Hash) (*types.Block, error) {
	nodes := c.nodes.HasBlock(hash)
	if len(nodes) == 0 {
		return c.nodes.Random().GetBlockByHash(ctx, hash)
	} else {
		return nodes[c.rand.Intn(len(nodes))].GetBlockByHash(ctx, hash)
	}
}

func (c *Cluster) getReceiptsByHash(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	nodes := c.nodes.HasBlock(hash)
	if len(nodes) == 0 {
		return c.nodes.Random().GetReceiptsByHash(ctx, hash)
	} else {
		return nodes[c.rand.Intn(len(nodes))].GetReceiptsByHash(ctx, hash)
	}
}

func (c *Cluster) getNodes(ctx context.Context, hash common.Hash) ([][]byte, error) {
	tile, state := c.tiler.GetTile(hash)
	if tile == nil || state == (common.Hash{}) {
		return nil, ErrNotFound
	}
	nodes := c.nodes.HasState(state)
	if len(nodes) == 0 {
		return nil, ErrNotFound
	}
	// Even the node has state and tile **is** referenced by the state,
	// the call can still return nil data seems the given state may be
	// wrong. In this case, the tile data is not available anyway.
	return nodes[c.rand.Intn(len(nodes))].GetNodes(ctx, tile.Hashes)
}
