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

import "github.com/ethereum/go-ethereum/log"

// NodeConfig includes all settings of node.
type NodeConfig struct {
	ID        string // The unique identity of node
	Endpoint  string // The RPC endpoint which enables the websocket
	RateLimit uint64 // The rate limit for sending requests to the node
}

//go:generate gencodec -type Config -formats toml -out gen_config.go
type Config struct {
	// Datadir the file path points to the database. If the value is empty,
	// an in-memory temporary database is applied.
	Datadir string

	// HTTPHost is the host interface on which to start the HTTP RPC server. If this
	// field is empty, no HTTP API endpoint will be started.
	HTTPHost string `toml:",omitempty"`

	// HTTPPort is the TCP port number on which to start the HTTP RPC server. The
	// default zero value is/ valid and will pick a port number randomly (useful
	// for ephemeral nodes).
	HTTPPort int `toml:",omitempty"`

	// Nodes is a list of node config(web socket endpoint).
	Nodes []NodeConfig

	// Log the logger for system, nil means default logger.
	Log log.Logger `toml:"-"`
}
