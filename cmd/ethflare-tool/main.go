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

package main

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"path"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/olekukonko/tablewriter"
	"github.com/rjl493456442/ethflare/database"
	"github.com/rjl493456442/ethflare/httpclient"
	"golang.org/x/crypto/sha3"
	"gopkg.in/urfave/cli.v1"
)

var (
	// Git SHA1 commit hash of the release (set via linker flags)
	gitCommit = ""
	gitDate   = ""
)

var app *cli.App

func init() {
	app = utils.NewApp(gitCommit, gitDate, "ethflare(ethereum data cdn)")
	app.Commands = []cli.Command{
		commandDBTool,
		commandNetworkTool,
	}
	app.Flags = []cli.Flag{}
	cli.CommandHelpTemplate = utils.OriginCommandHelpTemplate
}

// Commonly used command line flags.
var (
	dataDirFlag = utils.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the ethflare indexes and metadata",
		Value: utils.DirectoryString(path.Join(node.DefaultDataDir(), "ethflare")),
	}
	serverHostFlag = cli.StringFlag{
		Name:  "host",
		Value: "localhost",
		Usage: "The http endpoint server listens at",
	}
	serverPortFlag = cli.IntFlag{
		Name:  "port",
		Value: 9999,
		Usage: "The http endpoint server listens at",
	}
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	fdlimit.Raise(2048)

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var commandDBTool = cli.Command{
	Name:  "database",
	Usage: "A set of database tools for debugging",
	Subcommands: []cli.Command{
		{
			Name:  "list-tiles",
			Usage: "Iterate the tile database and list all content",
			Flags: []cli.Flag{
				dataDirFlag,
				cli.StringFlag{
					Name:  "root",
					Usage: "Specify the state root hash which iterates from",
					Value: "",
				},
			},
			Action: utils.MigrateFlags(listTiles),
		},
	},
}

var commandNetworkTool = cli.Command{
	Name:  "network",
	Usage: "A set of network tools for debugging",
	Subcommands: []cli.Command{
		{
			Name:  "list-chain",
			Usage: "Crawl all blocks via http",
			Flags: []cli.Flag{
				serverHostFlag,
				serverPortFlag,
				cli.IntFlag{
					Name:  "start",
					Usage: "Specify the start block number which iterates from",
					Value: 1,
				},
				cli.IntFlag{
					Name:  "end",
					Usage: "Specify the end block number which iterates to",
					Value: 1,
				},
				cli.StringFlag{
					Name:  "rpc",
					Usage: "Specify the rpc endpoint of Geth node",
				},
			},
			Action: utils.MigrateFlags(listChain),
		},
	},
}

func listTiles(ctx *cli.Context) error {
	datadir := ctx.String(dataDirFlag.Name)
	if datadir == "" {
		utils.Fatalf("no valid database specified")
	}
	db, err := database.NewDatabase(datadir)
	if err != nil {
		utils.Fatalf("Failed to open database %v", err)
	}
	root, _ := database.ReadStateRoot(db)
	if given := ctx.String("root"); given != "" {
		root = common.HexToHash(given)
	}
	if root == (common.Hash{}) {
		fmt.Println("Incomplete database")
		return nil
	}
	fmt.Printf("Inspecting(%x)...\n", root)
	var (
		tiles   uint64 // The total number of tile
		miss    uint64 // The total number of missing tile(broken db)
		hashes  uint64 // The total number of trie node
		refs    uint64 // The total number of references
		state   uint64 // The tile number of state trie
		storage uint64 // The tile number of storage trie

		maxHashes int
		maxRefs   int
		minHashes = math.MaxInt32
		minRefs   = math.MaxInt32

		tileSize common.StorageSize
		hashSize common.StorageSize
		refsSize common.StorageSize
	)
	queue := prque.New(nil)
	queue.Push(root, 0)
	for !queue.Empty() {
		hash, prio := queue.Pop()

		tile, _ := database.ReadTile(db, hash.(common.Hash))
		if tile == nil {
			miss += 1
			continue
		} else {
			tileSize += common.StorageSize(common.HashLength * (len(tile.Hashes) + len(tile.Refs)))
			hashSize += common.StorageSize(common.HashLength * len(tile.Hashes))
			refsSize += common.StorageSize(common.HashLength * len(tile.Refs))

			tiles += 1
			hashes += uint64(len(tile.Hashes))
			refs += uint64(len(tile.Refs))
			if len(tile.Hashes) > maxHashes {
				maxHashes = len(tile.Hashes)
			}
			if len(tile.Refs) > maxRefs {
				maxRefs = len(tile.Refs)
			}
			if len(tile.Hashes) < minHashes {
				minHashes = len(tile.Hashes)
			}
			if len(tile.Refs) < minRefs {
				minRefs = len(tile.Refs)
			}
			if tile.Depth < 64 {
				state += 1
			} else {
				storage += 1
			}
		}
		for _, ref := range tile.Refs {
			queue.Push(ref, prio+1)
		}
	}
	tileSize += common.StorageSize(tiles) // Depth uses 1 byte.

	// Display the database statistic.
	stats := [][]string{
		{"Tile size", tileSize.String()},
		{"Hash size", hashSize.String()},
		{"Refs size", refsSize.String()},
		{"Total", fmt.Sprintf("%d", tiles)},
		{"Miss", fmt.Sprintf("%d", miss)},
		{"State", fmt.Sprintf("%d", state)},
		{"Storage", fmt.Sprintf("%d", storage)},
		{"Avg nodes", fmt.Sprintf("%.2f", float64(hashes)/float64(tiles))},
		{"Avg refs", fmt.Sprintf("%.2f", float64(refs)/float64(tiles))},
		{"Max node", fmt.Sprintf("%d", maxHashes)},
		{"Max ref", fmt.Sprintf("%d", maxRefs)},
		{"Min node", fmt.Sprintf("%d", minHashes)},
		{"Min ref", fmt.Sprintf("%d", minRefs)},
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Category", "Size/Count"})
	table.AppendBulk(stats)
	table.Render()
	return nil
}

func listChain(ctx *cli.Context) error {
	var emptyUncle common.Hash
	hw := sha3.NewLegacyKeccak256()
	rlp.Encode(hw, []*types.Header(nil))
	hw.Sum(emptyUncle[:0])

	emptyTxs := types.DeriveSha(types.Transactions{})

	client := httpclient.NewClient(fmt.Sprintf("%s:%d", ctx.String(serverHostFlag.Name), ctx.Int(serverPortFlag.Name)))
	start, end := ctx.Int("start"), ctx.Int("end")

	rpc, err := rpc.Dial(ctx.String("rpc"))
	if err != nil {
		utils.Fatalf("Failed to connect rpc %v", err)
	}
	geth := ethclient.NewClient(rpc)

	var iterated int
	var now = time.Now()
	for i := start; i < end; i++ {
		block, err := geth.BlockByNumber(context.Background(), big.NewInt(int64(i)))
		if err != nil {
			utils.Fatalf("Failed to retrieve block %v", err)
		}
		hash := block.Hash()
		_, err = client.GetBlockHeader(context.Background(), hash)
		if err != nil {
			utils.Fatalf("Failed to retrieve header %v", err)
		}
		if block.UncleHash() != emptyUncle {
			_, err = client.GetUncles(context.Background(), hash)
			if err != nil {
				utils.Fatalf("Failed to retrieve uncles %v", err)
			}
		}
		if block.TxHash() != emptyTxs {
			_, err = client.GetTransactions(context.Background(), hash)
			if err != nil {
				utils.Fatalf("Failed to retrieve transactions %v", err)
			}
			_, err = client.GetReceipts(context.Background(), hash)
			if err != nil {
				utils.Fatalf("Failed to retrieve receipts %v", err)
			}
		}
		iterated += 1
		if iterated%10000 == 0 {
			log.Info("Iterated chain", "start", start, "now", i, "elapsed", common.PrettyDuration(time.Since(now)))
		}
	}
	return nil
}
