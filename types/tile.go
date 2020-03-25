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

package types

import (
	"io"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
)

// tile represents the metadata of a chunk of state trie.
type Tile struct {
	Depth uint8         // The depth of the root node in trie
	Nodes uint16        // The node number in tile
	Refs  []common.Hash // The references from tile to external
}

// EncodeRLP implements rlp.Encoder which flattens all necessary fields into an RLP stream.
func (t *Tile) EncodeRLP(w io.Writer) error {
	type tileRLP struct {
		Depth uint8         // The depth of the root node in trie
		Nodes uint16        // The node number in tile
		Refs  []common.Hash // The references from tile to external
	}
	return rlp.Encode(w, &tileRLP{t.Depth, t.Nodes, t.Refs})
}

// DecodeRLP implements rlp.Decoder which loads the persisted fields of a tile from an RLP stream.
func (t *Tile) DecodeRLP(s *rlp.Stream) error {
	type tileRLP struct {
		Depth uint8         // The depth of the root node in trie
		Nodes uint16        // The node number in tile
		Refs  []common.Hash // The references from tile to external
	}
	var dec tileRLP
	if err := s.Decode(&dec); err != nil {
		return err
	}
	t.Depth, t.Nodes, t.Refs = dec.Depth, dec.Nodes, dec.Refs
	return nil
}
