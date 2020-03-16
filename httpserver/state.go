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

package httpserver

import (
	"fmt"
	"net/http"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/log"
)

// serveState is responsible for serving HTTP requests for state data.
func (s *HTTPServer) serveState(w http.ResponseWriter, r *http.Request) {
	// Decode the root of the subtrie tile we should return
	root, err := hexutil.Decode(shift(&r.URL.Path))
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid state root: %v", err), http.StatusBadRequest)
		return
	}
	if len(root) != common.HashLength {
		http.Error(w, fmt.Sprintf("invalid state root: length %d != %d", len(root), common.HashLength), http.StatusBadRequest)
		return
	}
	hash := common.BytesToHash(root)
	out, err := s.cluster.ServeRequest(r.Context(), "GetNodes", hash)
	if err != nil {
		http.Error(w, fmt.Sprintf("non-existent state root %x", hash), http.StatusBadRequest)
		return
	}
	nodes := out.([][]byte)
	replyAndCache(w, nodes)
	log.Debug("Served state request", "root", hash)
}
