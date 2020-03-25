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
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/rjl493456442/ethflare/types"
)

func TestTileEncode(t *testing.T) {
	var tiles = []*types.Tile{
		{Depth: 0, Nodes: 1, Refs: nil},
		{Depth: 1, Nodes: 1, Refs: []common.Hash{common.HexToHash("cafe"), common.HexToHash("babe")}},
	}
	for _, d := range tiles {
		enc, err := rlp.EncodeToBytes(d)
		if err != nil {
			t.Fatal("Failed to encode tile")
		}
		var got types.Tile
		if err := rlp.DecodeBytes(enc, &got); err != nil {
			t.Fatal("Failed to decode tile")
		}
		if got.Depth != d.Depth {
			t.Fatal("Depth mismatch")
		}
		if got.Nodes != d.Nodes {
			t.Fatal("Hashes mismatch")
		}
		if len(got.Refs) != len(d.Refs) {
			t.Fatal("Refs mismatch")
		}
		for index, h := range got.Refs {
			if d.Refs[index] != h {
				t.Fatal("Refs mismatch")
			}
		}
	}
}
