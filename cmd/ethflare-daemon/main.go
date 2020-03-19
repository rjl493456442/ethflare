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

// ethflare-daemon offers a bunch of functionalities of serving and retrieval
// ethereum data(chain and state)
package main

import (
	"bufio"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path"
	"reflect"
	"runtime/debug"
	"syscall"
	"unicode"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/naoina/toml"
	"github.com/rjl493456442/ethflare/cluster"
	"github.com/rjl493456442/ethflare/httpserver"
	"gopkg.in/urfave/cli.v1"
	_ "net/http/pprof"
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
		commandServer,
	}
	app.Flags = []cli.Flag{
		serverHostFlag,
		serverPortFlag,
		dataDirFlag,
		configFileFlag,
		verbosityFlag,
	}
	cli.CommandHelpTemplate = utils.OriginCommandHelpTemplate
}

// Commonly used command line flags.
var (
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
	dataDirFlag = utils.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the ethflare indexes and metadata",
		Value: utils.DirectoryString(path.Join(node.DefaultDataDir(), "ethflare")),
	}
	configFileFlag = cli.StringFlag{
		Name:  "config",
		Usage: "TOML configuration file",
	}
	verbosityFlag = cli.IntFlag{
		Name:  "verbosity",
		Usage: "Logging verbosity: 0=crit, 1=error, 2=warn, 3=info, 4=debug, 5=trace",
		Value: 3,
	}
)

func main() {
	if err := app.Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func setlog(level log.Lvl) {
	log.Root().SetHandler(log.LvlFilterHandler(level, log.StreamHandler(os.Stderr, log.TerminalFormat(true))))
}

var commandServer = cli.Command{
	Name:  "server",
	Usage: "Start ethflare server for data indexing and serving",
	Flags: []cli.Flag{
		serverHostFlag,
		serverPortFlag,
		dataDirFlag,
		configFileFlag,
		verbosityFlag,
	},
	Action: utils.MigrateFlags(startServer),
}

// startServer creates a server instance and start serving.
func startServer(ctx *cli.Context) error {
	setlog(log.Lvl(ctx.Int(verbosityFlag.Name)))

	var config cluster.Config
	if file := ctx.GlobalString(configFileFlag.Name); file != "" {
		if err := loadConfig(file, &config); err != nil {
			utils.Fatalf("%v", err)
		}
	}
	if ctx.IsSet(dataDirFlag.Name) {
		config.Datadir = ctx.String(dataDirFlag.Name)
	}
	if ctx.IsSet(serverHostFlag.Name) {
		config.HTTPHost = ctx.String(serverHostFlag.Name)
	}
	if ctx.IsSet(serverPortFlag.Name) {
		config.HTTPPort = ctx.Int(serverPortFlag.Name)
	}
	if len(config.Nodes) == 0 {
		utils.Fatalf("No backend node specified")
	}
	// Initialize the ethflare cluster
	c, err := cluster.NewCluster(&config)
	if err != nil {
		log.Crit("Failed to start cluster", "err", err)
	}
	c.Start()

	server := httpserver.NewHTTPServer(c)
	go server.Start(config.HTTPHost, config.HTTPPort)
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	go func() {
		sigc := make(chan os.Signal, 1)
		signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
		defer signal.Stop(sigc)
		<-sigc
		log.Info("Got interrupt, shutting down...")
		go c.Stop()
		for i := 10; i > 0; i-- {
			<-sigc
			if i > 1 {
				log.Warn("Already shutting down, interrupt more to panic.", "times", i-1)
			}
		}
		debug.SetTraceback("all")
		panic("")
	}()
	c.Wait()
	return nil
}

func loadConfig(file string, cfg *cluster.Config) error {
	f, err := os.Open(file)
	if err != nil {
		return err
	}
	defer f.Close()

	err = tomlSettings.NewDecoder(bufio.NewReader(f)).Decode(cfg)
	// Add file name to errors that have a line number.
	if _, ok := err.(*toml.LineError); ok {
		err = errors.New(file + ", " + err.Error())
	}
	return err
}

// These settings ensure that TOML keys use the same names as Go struct fields.
var tomlSettings = toml.Config{
	NormFieldName: func(rt reflect.Type, key string) string {
		return key
	},
	FieldToKey: func(rt reflect.Type, field string) string {
		return field
	},
	MissingField: func(rt reflect.Type, field string) error {
		link := ""
		if unicode.IsUpper(rune(rt.Name()[0])) && rt.PkgPath() != "main" {
			link = fmt.Sprintf(", see https://godoc.org/%s#%s for available fields", rt.PkgPath(), rt.Name())
		}
		return fmt.Errorf("field '%s' is not defined in %s%s", field, rt.String(), link)
	},
}
