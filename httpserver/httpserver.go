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
	"path"
	"strings"

	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/rjl493456442/ethflare/cluster"
)

// HTTPServer the server used to accept requests from network.
type HTTPServer struct {
	// More additional mechanism can be applied here. Like
	// - Request authentication
	// - Listen address
	// - HTTP settings
	cluster *cluster.Cluster
}

func NewHTTPServer(cluster *cluster.Cluster) *HTTPServer {
	return &HTTPServer{cluster: cluster}
}

func (s *HTTPServer) Start(host string, port int) {
	address := fmt.Sprintf("%s:%d", host, port)
	log.Info("Ethflare http server started", "listen", address)
	http.ListenAndServe(address, newGzipHandler(s))
}

// ServeHTTP is the entry point of the les cdn, splitting the request across the
// supported submodules.
func (s *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch shift(&r.URL.Path) {
	case "chain":
		s.serveChain(w, r)
		return

	case "state":
		s.serveState(w, r)
		return
	}
	http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
}

// shift splits off the first component of p, which will be cleaned of relative
// components before processing. The returned head will never contain a slash and
// the remaining tail will always be a rooted path without trailing slash.
func shift(p *string) string {
	*p = path.Clean("/" + *p)

	var head string
	if idx := strings.Index((*p)[1:], "/") + 1; idx > 0 {
		head = (*p)[1:idx]
		*p = (*p)[idx:]
	} else {
		head = (*p)[1:]
		*p = "/"
	}
	return head
}

// replyAndCache marshals a value into the response stream via RLP, also setting caching
// to indefinite.
func replyAndCache(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Cache-Control", "max-age=31536000") // 1 year cache expiry
	if err := rlp.Encode(w, v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// reply marshals a value into the response stream via RLP but not caches it.
func reply(w http.ResponseWriter, v interface{}) {
	if err := rlp.Encode(w, v); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
