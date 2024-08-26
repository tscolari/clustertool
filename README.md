# Clustertool

This is an opinionated approach on a tool for building clusters,
or a "make anything a cluster" tool.

This is a work in progress.

Although all layers are exposed for now, the intention would be for
consumers to operate at the `App` level.

Anything that can be wrapped by the [FSM interface](https://github.com/hashicorp/raft/blob/main/fsm.go#L16) can be
converted into a cluster using this.

## TL;DR;

### App

App is a high-level wrapper over a `Node`.
It exposes only basic functionality, hiding most of the complexity.
An app is consensus and discovery aware:

It uses `hashicorp.serf` as the discovery tool. Through gossip,
every node becomes aware of new nodes, and when nodes are down.

It wires those messages with `hashicorp.raft` to automatically
join new nodes to the consensus.

It will keep in sync the state of raft with the current state
of serf.

Write commands can be performed on any node (leader or non-leader).
When a non-leader tasked with a write (`Apply`), it will internally
reroute that to the leader. All nodes will eventually reach consensus.


#### Usage

Each `App` instance is an unique node of a Cluster.

```golang

# Use a factory to create the app instance.

app, err := clustertool.Factory{}.
    WithContext(ctx).
    WithName("node-1").
    WithSerfConfig(serf.DefaultConfig()).
    WithRaftConfig(raft.DefaultConfig()).
    WithFSM(yourFSMImplementation).
    Build()

// It only needs to know about one node to join the cluster,
// it can be any node.
err := app.ConnectToNode(ipOfOneKnownNode)

// Expect this to redirect to the leader and block until a
// response is given back.
err := app.Apply(commandEncodedInBytes)

// It should have been also applied locally by the consensus at this point.
// Whatever your FSM is wrapping should have that write on it now.

```


### Node

Is contains more low-level wiring between the wrapped `Consensus` and `Discovery`
implementations.
It ensures that a consistent state is kept between nodes from `Discovery` and
nodes from `Consensus`.

### Consensus

Consensus is a wrapper over `hashicorp.Raft` package. It exposes the functionalities
that are necessary for the `Node` only.

### Discovery

Discovery is a wrapper over `hashicorp.Serf` package. It allows node discovery through
gossip, and exposes only the functionalities that are necessary for the `Node`.

## Future intentions

* Document more.
* Remove any `raft` or `serf` details from the exposed interfaces.
