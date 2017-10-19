[![Clojars Project](https://img.shields.io/clojars/v/luhhujbb/hbase-admin.svg)](https://clojars.org/luhhujbb/hbase-admin)

# clj-hbase-admin

A library to administer hbase

## Install in local maven repository

lein do clean, install

## The use in your project

	(:require [hbase.admin.core :as hbase])

## Example 1: Initialize connection

```clojure
(def conf (hbase/mk-hbase-config {:hbase.zookeeper.quorum "zk1:2181,zk2:2181,zk3:2181"
                                     :zookeeper.znode.parent "/hbase"}))
;;initialize connection in registry
(hbase/init-hbase-connection "my-hbase" conf)

;;get connection for "my-hbase"
(hbase/get-connection "my-hbase")
```

## Example 2: use admin interface

```clojure
(let [admin (hbase/get-admin (hbase/get-connection "my-hbase"))]
  ;;return list of table as HTableDescriptor
  (hbase/list-tables admin)

  ;;return tables name as string list
  (hbase/list-tables-name-as-string admin)

  ;;return cluster status
  (hbase/get-cluster-status admin)

  ;;list snapshot , return an HBaseProtos$SnapshotDescription object list
  (hbase/list-snapshots admin))

```

## Exmaple 3: table management

```clojure
(let [admin (hbase/get-admin (hbase/get-connection "my-hbase"))]
  ;;create an hbase table (column faily should be shorter , typically one char long)
  (hbase/create-table admin "my-table" ["my-family-1" "my-family-2"])

  ;;delete table
  (hbase/delete-table admin "my-table"))
```

## Example 4: make table snapshot

```clojure
(let [admin (hbase/get-admin (hbase/get-connection "my-hbase"))]
  ;;create a  snapshot of a table
  (hbase/snapshot admin "my-table" "my-snapshot")

  ;;Create a new table base on a snasphot
  (hbase/clone-snapshot admin "my-snapshot" "my-new-table")

  ;;Delete a snapshot
  (hbase/delete-snapshot admin "my-snapshot")

  ;;Snapshot-all : create a snapshot for all table
  ;;with "my-table-my-snapshot" as per table snapshot name
  (hbase/snapshot-all admin "my-snapshot"))
```

## TODO

test import/export from s3

## License

Copyright © 2017 Jean-Baptiste Besselat / Linkfluence SAS

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
