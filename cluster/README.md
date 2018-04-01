# Carbon Grid Cluster

The cluster package is provides services cluster membership change information, unique id assignment, access to consensus information, etc. It aims to make carbon grid operable without any additional consensus infrastructure such as zookeeper or etcd.

It does so by using two frameworks raft (the basis of etcd itself) and serf. In a nutshell, serf is used to broadcast the presence of members in the cluster. While raft is used to reach consensus over sensitive cluster information (such as unique ids).

## Serf

Serf is a lightweight gossip library for service discovery and orchestration. Carbon Grid uses it to discover all members of the cluster and (to some degree) announce role changes within the cluster. Check out of [the serf repo](https://github.com/hashicorp/serf) for more detailed information. Carbon Grid though only uses select packages of the library that can be found [here](https://github.com/hashicorp/serf/tree/master/serf).

## Raft

Raft is a single-master store that is used to coordination of tasks that need consensus between different members. It comes wrapped as a [library](https://github.com/hashicorp/raft). It practically implements a strongly consistent, distributed data store. The library does so by implementing the [raft protocol](http://thesecretlivesofdata.com/raft/). While (at least) two different raft protocol implementations exist (coreos and hashicorp), Carbon Grid uses the hashicorp implementation. This way the consensus store implements a single-master, strongly-consistent, distributed key-value store. Both keys and values are typed, where keys are strings and values byte arrays containing arbitrary data.

## The Cluster Interface

The cluster interface is the only public API of this package providing the ability to subscribe to changes in cluster membership and acquiring a cluster-wide unique short (integer) id.

### Changes in Cluster Membership

Serf shines at node discovery and gossiping and that's what Carbon Grid uses it for. New nodes are seeded with a (or a few) existing nodes to contact. After contact has been made, cluster state is traded and exchanged. All additions and removals of nodes is communicated out to a channel as cluster change events. The cache component then can react by keeping its node state up to date.

### Short, unique Ids

Grid communication happens on basis of short, unique cluster member ids. The cache component only thinks in terms of these short node ids. Internally to the cluster component uses [wide string ids](https://github.com/oklog/ulid) as unique member id. These ids *have to be unique* for serf and raft to work properly. Each node has exactly one short and one wide id (both of which are unique).

## The Cluster Implementation

As the duties of the cluster package are distributed between multiple other components, bringing a new node into the cluster requires a somewhat careful choreography of steps.

That means when a cluster starts, the components are started in the following order:
* start consensus (raft)
  * the new raft instance will not connected to anything quite yet
  * eventually the raft leader will decide whether any new node should be promoted to voter or nonvoter (based on the number of nodes already fulfilling these roles)
* start membership (serf)
  * join the current cluster or form a new one
  * if we join an existing cluster, process all events
  * find all the metadata required for operation
    * that would mostly be the raft roles in the cluster (leader, voters, nonvoters)
* start raft service
  * raft service makes all operations of the consensus store available to every node by forwarding the operations to the respective raft leader in the cluster
  * forwarding to the raft cluster across node failures is transparent

Most of the clusters metadata is kept track of in the consensus store (strongly consistent) and the membership store (vaguely consistent) at the same time.
The metadata exists in two forms:
1. as byte array (protobuf serialized node info) in the consensus store
2. as multiple map[string]string in the membership store

Both forms include all information necessary to manage the cluster, the membership to clusters and specific roles and tasks that need to be fulfilled within the cluster.
  * host: the advertised host name of this node
  * serf_port: the port on which serf for this node operates
  * raft_port: the port on which raft for this node operates
  * raft_service_port: the port on which the raft service for this node operates
  * raft_role: leader, voter, nonvoter - the role a particular node has in the raft cluster
  * grid_port: the address on which the grid messages are being exchanged

The data layout in both the membership and consensus store has a key-value format. The majority of data in membership is available as map[string]string. While information in consensus is a loose collection of keys and associated values. The keys however follow conventions that make deriving these keys possible. All naming conventions are defined in cluster.go as constants. Keep in mind that even though these keys read like directories, they are not! The data layout for consensus looks as follows:
* individual keys for each member in the cluster of the form "carbon-grid/members/<long member id>"
  * the value behind each of these keys is a byte array corresponding to a node info object
* three individual lists of raft leaders, raft voters, and raft nonvoters

The consensus store keeps the state of the cluster under a handful of keys. The names and strings are defined in the [cluster file](mhelmich/carbon-copy-go/cluster/cluster.go#L78):
* each cluster member has its member info under its long member id
* there's a key for the raft leader
* a prefix for raft voters
* a prefix for raft nonvoters
