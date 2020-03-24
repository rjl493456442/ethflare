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

// httpclient offers a client can interactive with ethflare.
package httpclient

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rlp"
)

// Client is an HTTP client which connects the ethflare server.
type Client struct {
	// URL specifies the ethflare server endpoint the client
	// wants to connect.
	URL string

	// Timeout specifies a time limit for requests made by this
	// Client. A Timeout of zero means no timeout.
	Timeout time.Duration
}

func NewClient(url string) *Client { return &Client{URL: url} }

func (client *Client) GetBlockHeader(ctx context.Context, hash common.Hash) (*types.Header, error) {
	var header types.Header
	res, err := httpDo(ctx, fmt.Sprintf("%s/chain/0x%x/header", client.URL, hash))
	if err != nil {
		return nil, err
	}
	if err := rlp.DecodeBytes(res, &header); err != nil {
		return nil, err
	}
	return &header, nil
}

func (client *Client) GetUncles(ctx context.Context, hash common.Hash) ([]*types.Header, error) {
	var headers []*types.Header
	res, err := httpDo(ctx, fmt.Sprintf("%s/chain/0x%x/uncles", client.URL, hash))
	if err != nil {
		return nil, err
	}
	if err := rlp.DecodeBytes(res, &headers); err != nil {
		return nil, err
	}
	return headers, nil
}

func (client *Client) GetTransactions(ctx context.Context, hash common.Hash) (types.Transactions, error) {
	var txs types.Transactions
	res, err := httpDo(ctx, fmt.Sprintf("%s/chain/0x%x/transactions", client.URL, hash))
	if err != nil {
		return nil, err
	}
	if err := rlp.DecodeBytes(res, &txs); err != nil {
		return nil, err
	}
	return txs, nil
}

func (client *Client) GetReceipts(ctx context.Context, hash common.Hash) (types.Receipts, error) {
	var receipts types.Receipts
	res, err := httpDo(ctx, fmt.Sprintf("%s/chain/0x%x/receipts", client.URL, hash))
	if err != nil {
		return nil, err
	}
	if err := rlp.DecodeBytes(res, &receipts); err != nil {
		return nil, err
	}
	return receipts, nil
}

func (client *Client) GetStateTile(ctx context.Context, hash common.Hash) ([][]byte, error) {
	res, err := httpDo(ctx, fmt.Sprintf("%s/state/0x%x?target=%d&limit=%d&barrier=%d", client.URL, hash, 16, 256, 2))
	if err != nil {
		return nil, err
	}
	var nodes [][]byte
	if err := rlp.DecodeBytes(res, &nodes); err != nil {
		return nil, err
	}
	return nodes, nil
}

// httpDo sends a http request based on the given URL, extracts the
// response and return.
func httpDo(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	blob, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}
	res.Body.Close()
	return blob, nil
}
