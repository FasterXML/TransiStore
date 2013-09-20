# TransiStore

TransiStore is a distributed data store for temporary (time-bound, possibly but not necessarily transient) data, such as intermediate processing results (for Map/Reduce, Hadoop), staging area between high-volume producers and consumers (log processing and aggregation), or just as general-purpose store for data exchange.

TransiStore is built on [ClusterMate](https://github.com/cowtowncoder/ClusterMate) platform, and serves as a sample system.
Note that implementing storage system with different behavior, using `ClusterMate` (possibly using `TransiStore` as sample code) is highly encouraged: it is not meant as "The" storage system; although if it works as-is for your use case, all the better.

## Basics

### License

'TransiStore` (and all its dependencies) are licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

### Functionality

`TransiStore` can be viewed as a distributed key/value ("BLOB") store which explicitly supports Key Range queries for primary keys.
Stored entries have explicit time-to-live setting and service removes expired entries if they have not been explicitly deleted earlier.
Clusters are horizontally scalable and allow addition/removal of storage nodes, without restarting other nodes.

The main limitation -- as of now, at least -- is that content is Write-Once; that is, entries are immutable after being added: they may however. be explicitly deleted (and if not, will expire as per time-to-live settings). While this is a limit that use cases must conform to, it greatly simplifies implementation and improves handling performance, as conflict resolution is simple to handle (due to minimal number of cases to resolve).

### Underlying (per-node) storage

Storage layer comes from [ClusterMate](https://github.com/cowtowncoder/ClusterMate) (which in turn builds on [StoreMate](https://github.com/cowtowncoder/StoreMate) project.

Here are some highlights (for more refer to `StoreMate` project) of storage system:

* Pluggable backends: default implementation uses `BDB-JE` for local storage, but there is also experimental `LevelDB` backend.
* Automatic on-the-fly (de)compression; negotiated using standard HTTP; supports multiple compression methods (client can pre-compress instead of server, or defer decompression)
* Partial content queries (HTTP Range supported)
* Key-range queries.

and additional features that `ClusterMate` provides are:

* Peer-to-peer content synchronization used for on-going content synchronization, recovery, and bootstrapping of newly added nodes
* Configurable redundancy (number of copies to store), with different client-controlled minimal required writes.
* Client-configurable data expiration rates (per-entry time-to-live) to ensure that content will not live forever even if no explicit deletions are performed
* Key partitioning to support cluster-wide key-range queries (routing by partition; queries within single partition) -- note: key structure fully configurable at `ClusterMate` level; TransiStore uses a simple `partition + path` key structure.

### Configuration

Configuration is simple: it consists of a single JSON configuration file. Sample configuration
files can be found from under `sample/`.

## Why not X?

(where X may be "HBase", "Cassandra", "Voldemort", "Riak" or any of dozens of distributed key/value stores)

Features that separate TransiStore for most other "noSQL" key/value stores are ones listed earlier:

1. Can store LARGE entries (similar to S3): authors use it for storing 10 megabyte sized entries. It is designed for such usage -- you can even do partial GETs (with HTTP Range header)
2. Key-range queries: some noSQL systems (like HBase) support them, but most don't (Cassandra, Voldemort), at least with recommended set ups. TransiStore is designed to  fully support them.
3. Automatic expiration of data: useful for intermediate results (Hadoop processing? Log aggregation?), weekly reports, or even some types of caching. You just specify maximum life-time for entries and system purges them as necessary
4. Simple configuration: single configuration file, used for all nodes; and even clients if you want.
5. Automatic node recovery: once a node has its configuration file, it is ready to start up and synchronize entries it needs -- no manual process or delicate coordination needed (NOTE: there will be need to build dashboards, tools to help track progress -- but foundational functionality works for actual syncing)

There are many more advanced features; but most of them are common within this class of servers
(recovery, configurable number of copies required, statistics/metrics).

## Documentation

Here are things you will probably want to read first:

* [Installing](../../wiki/Install) (my first TransiStore cluster): Build or download jar, find a config, start up!
* [Configuration](../../wiki/Configuration): what do those sample configs contain?
* [Tools](../../wiki/Tools): Yes, we have small set of command-line tools too; accessed with `tstore.sh` wrapper (invoking `ts-client` jar)

and here's some more stuff:

* [Dependencies](../../wiki/Dependencies) -- what is TransiStore built of?
