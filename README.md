## Ethflare

Ethflare is a data hosting and distribution solution for ethereum.

As we all know, a major feature of the blockchain is that the data is tamper-resistant. To be more specified, whenever a block is generated, the content will never be changed even it's not caonical block eventually. It's identified by an unique hash. What's more, serveral blockchain data types can all be identified by hash like.

- Block header
- Block body(transactions and uncles)
- Receipts

We can combine this character with tranditional infrastructure **CDN**. All imutable data can be the static resources hosted and distributed by CDN network. This is what ethflare for "A CDN solution for ethereum".

After having ethflare, Ethereum's light clients can not only obtain the required data through the p2p network of Ethereum, but also can connect to one or more ethflare service providers by "try for free" or "pay for use" to obtain *stable and fast* data services.

Blockchain data is easy to understand and handle. However ethereum state data is more troublesome.

Although ethereum state data is also immutable. Whenever a state transition is finished, a version of trie is created. All trie nodes referenced by this state are immutable.

But for light nodes, the most common use case for state is merkle proof. Merkle proof data is a collection of tree nodes that starts from the root node to the leaf nodes. However, it is foolish to directly treat the merkle proof as a static resource.

First, Ethereum's status data is updated very frequently. Each block will cause thousands of nodes to be de-referenced and thousands of new nodes to be inserted. However, light nodes will only use the latest state root to make requests, so it makes no sense to cache "outdated" merkle proofs. Secondly, the light client usually only cares about its own data, such as the balance of an account. It is also meaningless to cache merkle proof directly.

So ethflare will "tiling" Ethereum state data. In simple terms, the entire state is divided into chunks according to a specific algorithm. Each chunks can be seen as the "top part" of a subtrie. Each chunk(or tile) is uniquely identified based on the root hash of this subtrie.

Through state tiling, the repeated utilization of each state data can be maximized, and it can also be hosted as an effective static resource.

### Installation

```shell
go get https://github.com/rjl493456442/ethflare.git
```

### Usage

**Start ethflare daemon**

```shell
ethflare-daemon server --config <YOUR_CONFIG>
```









