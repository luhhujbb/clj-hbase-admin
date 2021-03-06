[![Clojars Project](https://img.shields.io/clojars/v/luhhujbb/hbase-admin.svg)](https://clojars.org/luhhujbb/hbase-admin)

# clj-hbase-admin

A library to administer hbase

## Install in local maven repository

lein do clean, install

## The use in your project

	(:require [hbase.admin.core :as hbase])

## Example 1: Initialize connection

```clojure
;; you can create a configuration from a map
(def conf (hbase/mk-hbase-config {:hbase.zookeeper.quorum "zk1:2181,zk2:2181,zk3:2181"
                                     :zookeeper.znode.parent "/hbase"}))

;;you can also load hadoop/hbase confs (this is better if you want to use export/import features)
(def conf (hbase/mk-hbase-config "/etc/hadoop/hdfs-site.xml,/etc/hadoop/core-site.xml,/etc/hbase/hbase-site.xml"))

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

## Example 3: get cluster load

```clojure
(let [admin (hbase/get-admin (hbase/get-connection "my-hbase"))]
  ;;return basic cluster status and global load
  (hbase/get-cluster-status admin)

  ;;return detailed servers load including regions load
  (hbase/get-servers-load admin)

  ;;return detailed regions load (extrated from get-servers-load)
  (hbase/get-regions-load admin)

	;;shortcut function with interesting grouping from get-server-load

  ;;return detailed regions load for a single server
  (hbase/get-server-regions-load admin server)

	;;return detailed regions load for a single table
  (hbase/get-table-regions-load admin table-name)

	;;return detailed tables load per regions server
  (hbase/get-servers-tables-load admin)

	;;return detailed tables load for a single server
  (hbase/get-server-tables-load admin server)

	;;return detailed tables load
  (hbase/get-tables-load admin server))
```

## Exmaple 4: table management

```clojure
(let [admin (hbase/get-admin (hbase/get-connection "my-hbase"))]
  ;;create an hbase table (column faily should be shorter , typically one char long)
  (hbase/create-table admin "my-table" ["my-family-1" "my-family-2"])

	;;compact a table
	(hbase/compact-table admin "my-table")

	;;major compact a table
	(hbase/major-compact-table admin "my-table")

	;;get compaction state of table
	(hbase/table-compaction-state admin "my-table")

  ;;delete table
  (hbase/delete-table admin "my-table"))
```

## Example 5: make table snapshot

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

## Example 6: basic put/get/delete operations

```clojure
;;create row
(hbase/put-row
	(hbase/get-connection "my-hbase")
	"my-table"
	(.getBytes "my-row-key")
	[{:family (.getBytes "my-family-1")
		:qualifier (.getBytes "my-qualifier-1")
		:value (.getBytes "my-value-1")}
	 {:family (.getBytes "my-family-1")
  	:qualifier (.getBytes "my-qualifier-1")
  	:value (.getBytes "my-value-1")}])

;;get-row
(hbase/get-row
	(hbase/get-connection "my-hbase")
	"my-table"
	(.getBytes "my-row-key"))

;;delete row
(hbase/delete-row
	(hbase/get-connection "my-hbase")
	"my-table"
	(.getBytes "my-row-key"))
```

## Example 7: exportSnapshot
```clojure

(let [admin (hbase/get-admin (hbase/get-connection "my-hbase"))
			export-options {:s3-protocol "s3a://"
					:access-key "****"
					:secret-key "****"
					:bucket "my-bucket"
					:path "/my-custom-path"}]

;;Export data to s3
	(export-snapshot-to-s3 "my-hbase" "my-snapshot" export-options)
;;Delete snapshot
	(delete-snapshot admin "my-snapshot")
;;re-Import data from s3
	(import-snapshot-from-s3 "my-hbase" "my-snapshot" export-options))
```

## Thanks

Fix to export hbase table to s3 [@nenorbot](https://github.com/nenorbot)

## TODO

* fix tls error in leiningen 2.8.1

## License

Copyright © 2017 Jean-Baptiste Besselat / Linkfluence SAS

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
