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
	"fmt"
	"os"
	"path"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/fdlimit"
	"github.com/ethereum/go-ethereum/common/math"
	"github.com/ethereum/go-ethereum/common/prque"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/olekukonko/tablewriter"
	"github.com/rjl493456442/ethflare/database"
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
		commandInspect,
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
)

func main() {
	log.Root().SetHandler(log.LvlFilterHandler(log.LvlDebug, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
	fdlimit.Raise(2048)

	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

var commandInspect = cli.Command{
	Name:  "inspect",
	Usage: "Inspect tile database for many purposes",
	Subcommands: []cli.Command{
		{
			Name:   "list",
			Usage:  "Iterate the tile database and list all content",
			Flags:  []cli.Flag{dataDirFlag},
			Action: utils.MigrateFlags(listTiles),
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
	if root == (common.Hash{}) {
		fmt.Println("Uncompleted database")
		return nil
	}
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
