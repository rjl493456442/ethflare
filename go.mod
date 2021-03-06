module github.com/rjl493456442/ethflare

go 1.13

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/ethereum/go-ethereum v1.9.11
	github.com/naoina/toml v0.1.2-0.20170918210437-9fafd6967416
	github.com/olekukonko/tablewriter v0.0.2-0.20190409134802-7e037d187b0c
	golang.org/x/crypto v0.0.0-20190308221718-c2843e01d9a2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4
	gopkg.in/urfave/cli.v1 v1.20.0
)

replace github.com/ethereum/go-ethereum v1.9.11 => ../../ethereum/go-ethereum
