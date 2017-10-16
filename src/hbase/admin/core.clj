(ns hbase.admin.core
  (:require [clojure.java [io :as io]]
            [clojure.tools.logging :as log])
  (:import [java.io InputStream]
           [org.apache.hadoop.util Tool]
           [org.apache.hadoop.util ToolRunner]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.conf Configuration Configured]
           [org.apache.hadoop.hbase HBaseConfiguration TableName Cell CellUtil HTableDescriptor]
           [org.apache.hadoop.hbase.client ConnectionFactory Admin Get Table Result]
           [org.apache.hadoop.hbase.protobuf.generated HBaseProtos$SnapshotDescription]
           [org.apache.hadoop.hbase.snapshot ExportSnapshot	SnapshotCreationException]))

(def hbase-general-config (atom {}))

(def hbase-connection-registry (atom {}))

(def hbase-config-registry (atom {}))

(def s3-default-protocol "s3n://")

(defmulti mk-hbase-config
            (fn [x] (cond
              (instance? java.io.InputStream x) :input-stream
              (instance? clojure.lang.PersistentArrayMap x) :map
              (string? x) :file-path
              :else :not-supported)))

(defmethod mk-hbase-config :map [properties]
  (let [^HBaseConfiguration conf (HBaseConfiguration/create)]
    (doseq [[k v] properties]
      (.set conf (name k) v))
    conf))

(defmethod mk-hbase-config :input-stream [^InputStream input-stream]
  (let [^HBaseConfiguration conf (HBaseConfiguration/create)]
    (.addResource conf input-stream)
    conf))

(defmethod mk-hbase-config :file-path [^String file-path]
  (let [^HBaseConfiguration conf (HBaseConfiguration/create)]
    (.addResource conf (io/input-stream file-path))
    conf))

(defmulti update-hbase-config
            (fn [_ x] (cond
              (instance? java.io.InputStream x) :input-stream
              (instance? clojure.lang.PersistentArrayMap x) :map
              (string? x) :file-path
              :else :not-supported)))

(defmethod update-hbase-config :map [^HBaseConfiguration conf properties]
    (doseq [[k v] properties]
      (.set conf (name k) v))
    conf)

(defmethod update-hbase-config :input-stream [^HBaseConfiguration conf ^InputStream input-stream]
    (.addResource conf input-stream)
    conf)

(defmethod update-hbase-config :file-path [^HBaseConfiguration conf ^String file-path]
    (.addResource conf (io/input-stream file-path))
    conf)

(defn init-hbase-connection
  "Make a connection and store it into registry"
  [name ^HBaseConfiguration conf]
  (try
    (let [connection (ConnectionFactory/createConnection conf)]
      (swap! hbase-config-registry assoc name conf)
      (swap! hbase-connection-registry assoc name connection))
    (catch java.io.IOException e
      (log/error "Failed to get hbase connection"))))

(defn init-sc-hbase-connection
  "Shortcut (sc) method with only quorum && base-path"
  [quorum & [base-path]]
  (let [base-path (if base-path base-path "/hbase")]
      (let [config (mk-hbase-config {:hbase.zookeeper.quorum quorum
                                     :zookeeper.znode.parent base-path})]
        (init-hbase-connection (str quorum base-path) config))))

(defn get-connection
  "Return a connection given a hbase name"
  [name]
  (if-let [connection (get-in @hbase-connection-registry name nil)]
    (if (.isClosed connection)
      (do
        (mk-hbase-config name (get-in @hbase-connection-registry name))
        (get-in @hbase-connection-registry name nil))
      connection)
    (log/error "No connection found for this cluster")))

(defn get-admin
  "Retrieve an Admin implementation to administer an HBase cluster."
  [connection]
  (.getAdmin connection))

(defn get-table
  "Retrieve a Table implementation for accessing a table."
  [connection table-name]
  (.getTable connection (TableName/valueOf table-name)))

;;Tables

(defn list-tables-names
  "List all of the names of userspace tables."
  [^Admin admin]
  (.listTableNames admin))

(defn list-tables-names-as-string
  [^Admin admin]
  "List all of the names as string of userspace tables."
  (into [] (map (fn [x] (.getNameAsString x)) (.listTableNames admin))))

(defn get-table-details
  ([^HTableDescriptor table]
    {:name (.getNameAsString table)
     :column-families (into [] (map (fn [x]
                                          {:name (.getNameAsString x)
                                           :compression (.getName
                                                          (.getCompression x))
                                           :bloomfilter (.toString
                                                          (.getBloomFilterType x))}) (.getFamilies table)))})
  ([^Admin admin table-name]
   (get-table-details (.getTableDescriptor admin (TableName/valueOf table-name)))))

 (defn list-tables-details
   "List Tables with details"
   [^Admin admin]
   (into []
     (map get-table-details (.listTables admin))))

;;Row

(defn exist?
  "Indicate if a row exists or not"
  [connection table-name row-key]
  (let [^Table table (get-table connection table-name)
        ^Get get (Get. row-key)]
    (.exists table get)))

(defn get-row
  [connection table-name row-key]
  (let [^Table table (get-table connection table-name)
        ^Get get-specs (Get. row-key)]
    (try
      (let [^Result result (.getRow table get-specs)
            ^List<Cell> cells (.listCells result)]
            )
      (catch Exception e
        (log/error "Error while getting row" e)))))

;;Snapshots

(defn list-snapshots
  "List completed snapshots."
  [^Admin admin]
  (.listSnapshots admin))

(defn snapshot
  "Create a timestamp consistent snapshot for the given table"
  [^Admin admin table-name snapshot-name]
  (.snapshot admin table-name snapshot-name))

(defn delete-snapshot
  "Delete an existing snapshot."
  [^Admin admin table-name snapshot-name]
  (.deleteSnapshot admin snapshot-name))

(defn restore-snapshot
  "Restore the specified snapshot on the original table."
  [^Admin admin snapshot-name]
  (.restoreSnapshot admin snapshot-name))

(defn clone-snapshot
  "Create a new table by cloning the snapshot content."
  [^Admin admin snapshot-name table-name]
  (.cloneSnapshot admin snapshot-name (TableName/valueOf table-name)))

(defn- mk-s3url
  [with-creds? with-path? opts]
  (if with-creds?
    (str (:s3-protocol opts) (:access-key opts) ":" (:secret-key opts) + "@" (:bucket opts) (when with-path? (:path opts)))
    (str (:s3-protocol opts) (:bucket opts) (when with-path? (:path opts)))))

(defn- mk-toolrunner-args
  [snapshot-name url parallelism]
  (into-array ["-snaspshot" snapshot-name "-copy-to" url "-mappers" (if (integer? parallelism) (.toString parallelism) parallelism)]))

(defn- mk-tool-runner-config
  [conf opts]
  (let [properties
   {:fs.default.name (mk-s3url true false opts)
    :fs.defaultFS (mk-s3url true false opts)
    :fs.s3.awsAccessKeyId (:access-key opts)
    :fs.s3.awsSecretAccessKey (:secret-key opts)
    :hbase.tmp.dir "/tmp/hbase-${user.name}"
    :hbase.rootdir (mk-s3url true true opts)}]
    (update-hbase-config conf properties)))

(defn export-snapshot-to-s3
  [name snapshot-name opts])

(defn import-snapshot-from-s3
  [name snapshot-name opts])
