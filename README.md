# TransiStore

TransiStore is a distributed data store for temporary (time-bound, possibly but not necessarily transient) data, such as intermediate processing results (for Map/Reduce, Hadoop), staging area between high-volume procuders and consumers (log processing and aggregation), or just as general-purpose store for data exchange.

TransiStore is built on [ClusterMate](https://github.com/cowtowncoder/ClusterMate) platform, and serves as a sample system.
Note that implementing storage system with different behavior, using `ClusterMate` (possibly using `TransiStore` as sample code) is highly encouraged: it is not meant as "The" storage system; although if it works as-is for your use case, all the better.

## Basics

'TransiStore` (and all its dependencies) are licensed under [Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

`TransiStore` can be viewed as a distributed key/value ("BLOB") store which explicitly supports Key Range queries for primary keys. It is horizontally scalable and allows addition/removal of storage nodes, without node restarts.

The main limitation -- as of now, at least -- is that content is Write-Once; that is, entries are immutable after being added: they may however. be explicitly deleted (and if not, will expire as per time-to-live settings). While this is a limit that use cases must conform to, it greatly simplifies implementation and improves handling performance, as conflict resolution is simple to handle (due to minimal number of cases to resolve).

Storage layer comes from `ClusterMate` (which in turn builds on [StoreMate](https://github.com/cowtowncoder/StoreMate).
Here are some highlights (for more refer to `StoreMate` project):

* Pluggable backends: default implementations uses `BDB-JE` for local storage
* Automatic on-the-fly (de)compression; negotiated using standard HTTP; supports multiple compression methods
* Partial content queries (HTTP Range supported)


## Documentation

First things you are likely to read are these:

* [Installing](../../wiki/Install)
* [Tools](../../wiki/Tools)
* [Configuration](../../wiki/Configuration)
