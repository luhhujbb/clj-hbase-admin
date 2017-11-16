(ns hbase.admin.core
  (:require [clojure.java [io :as io]]
            [clojure.tools.logging :as log]
            [clojure.java.data :refer [from-java]])
  (:import [java.io InputStream]
           [org.apache.hadoop.util Tool]
           [org.apache.hadoop.util ToolRunner]
           [org.apache.hadoop.hbase.util Bytes]
           [org.apache.hadoop.conf Configuration Configured]
           [org.apache.hadoop.hbase HBaseConfiguration TableName Cell
           CellUtil HTableDescriptor HColumnDescriptor KeyValue KeyValue$Type
           ClusterStatus ServerLoad]
           [org.apache.hadoop.hbase.client ConnectionFactory Admin Get Put Delete Increment Table Result]
           [org.apache.hadoop.hbase.protobuf.generated HBaseProtos$SnapshotDescription]
           [org.apache.hadoop.hbase.snapshot ExportSnapshot	SnapshotCreationException]
           [org.rtgi.hbase.admin HBaseClusterStatus]))

(def hbase-connection-registry (atom {}))

(def hbase-config-registry (atom {}))

(def cell-type #{"Minimum" "Put" "Delete" "DeleteFamilyVersion" "DeleteColumn" "DeleteFamily" "Maximum"})

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
  [hbase-name]
  (if-let [connection (get @hbase-connection-registry hbase-name nil)]
    (if (.isClosed connection)
      (do
        (init-hbase-connection hbase-name (get @hbase-config-registry hbase-name))
        (get @hbase-connection-registry hbase-name nil))
      connection)
    (log/error "No connection found for this cluster")))


(defn get-config
  "Return an hbase config"
  [hbase-name]
  (get @hbase-config-registry hbase-name))

(defn get-admin
  "Retrieve an Admin implementation to administer an HBase cluster."
  [connection]
  (.getAdmin connection))

(defn get-table
  "Retrieve a Table implementation for accessing a table."
  [connection table-name]
  (.getTable connection (TableName/valueOf table-name)))

;;Cluster

(defn get-cluster-status
  [^Admin admin]
  (from-java (.getClusterStatus admin)))

;;Tables

(defn list-tables
  "List all tables, return HTableDescriptor object array"
  [^Admin admin]
  (.listTables admin))

(defn list-tables-name
  "List all of the names of userspace tables."
  [^Admin admin]
  (.listTableNames admin))

(defn list-tables-name-as-string
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

(defn disable-table
  "Disable Table"
  [^Admin admin table-name]
  (.disableTable admin (TableName/valueOf table-name)))

(defn delete-table
  "Delete Table"
  [^Admin admin table-name]
  (disable-table admin table-name)
  (.deleteTable admin (TableName/valueOf table-name)))

(defn create-table
  "Create table"
  [^Admin admin table-name column-families]
  (let [^HTableDescriptor table (HTableDescriptor. (TableName/valueOf table-name))]
    (doseq [family column-families]
      (.addFamily table (HColumnDescriptor. family)))
    (.createTable admin table)))

;;Row

(defn cell->map
  [^Cell cell]
  {:row (CellUtil/cloneRow cell)
   :family (CellUtil/cloneFamily cell)
   :qualifier (CellUtil/cloneQualifier cell)
   :value (CellUtil/cloneValue cell)
   :timestamp (.getTimestamp cell)
   :type (.name (KeyValue$Type/codeToType (.getTypeByte cell)))})

(defn map->cell
  [mcell]
  (CellUtil/createCell
    (:row mcell)
    (:family mcell)
    (:qualifier mcell)
    (:timestamp mcell)
    (.getCode (KeyValue$Type/valueOf (:type mcell)))
    (:value mcell)))

(defn exist?
  "Indicate if a row exists or not"
  [connection table-name row-key]
  (let [^Table table (get-table connection table-name)
        ^Get get (Get. row-key)
        res (.exists table get)]
    (.close table)
    res))

(defn put-row
  "Put a row to a table"
  [connection table-name row-key mcells & [ts?]]
  (let [^Table table (get-table connection table-name)
        ^Put put-specs (Put. row-key)]
        (doseq [mcell mcells]
          (if-not ts?
            (.addColumn put-specs (:family mcell) (:qualifier mcell) (:value mcell))
            (.addColumn put-specs (:family mcell) (:qualifier mcell) (:timestamp mcell) (:value mcell))))
        (try
          (.put table put-specs)
          (.close table)
          (catch Exception e
            (log/error "Error while putting row" e)))))

(defn inc-row
  "Increments one or more columns within a single row.
  map-cells (mcells) value is converted to amount if amount keyword is absent"
  [connection table-name row-key mcells]
  (let [^Table table (get-table connection table-name)
        ^Increment inc-specs (Increment. row-key)]
        (doseq [mcell mcells]
          (when (integer? (or (:amount mcell) (:value mcell)))
            (.addColumn inc-specs
              (:family mcell)
              (:qualifier mcell)
              (long (or (:amount mcell) (:value mcell))))))
        (.increment table inc-specs)
        (.close table)))

(defn get-row
  "Retrieve a row"
  [connection table-name row-key]
  (let [^Table table (get-table connection table-name)
        ^Get get-specs (Get. row-key)]
    (try
      (let [^Result result (.get table get-specs)
            ^List<Cell> cells (.listCells result)
            _ (.close table)]
            (map cell->map cells))
      (catch Exception e
        (log/error "Error while getting row" e)
        "error"))))

(defn delete-row
  "Delete a row"
  [connection table-name row-key & [mcells ts?]]
  (let [^Table table (get-table connection table-name)
        ^Delete del-specs (Delete. row-key)]
        (when mcells
          (doseq [mcell mcells]
          (if-not ts?
            (.addColumns del-specs (:family mcell) (:qualifier mcell))
            (.addColumn del-specs (:family mcell) (:qualifier mcell) (:timestamp mcell)))))
    (try
          (.delete table del-specs)
          (.close table)
      (catch Exception e
        (log/error "Error while deleting row" e)))))

;;Snapshots

(defn list-snapshots
  "List completed snapshots."
  [^Admin admin]
  (.listSnapshots admin))

(defn list-snapshots-name
  [^Admin admin]
  (map (fn [^HBaseProtos$SnapshotDescription x] (.getName x)) (.listSnapshots admin)))

(defn snapshot
  "Create a timestamp consistent snapshot for the given table"
  [^Admin admin table-name snapshot-name]
  (.snapshot admin snapshot-name (TableName/valueOf table-name)))

(defn snapshot-all
  "Create a timestamp consistent snapshot for all the tables"
  [^Admin admin snapshot-name]
  (let [tables-name (list-tables-name-as-string admin)]
    (doseq [tn tables-name]
      (snapshot admin tn (str tn "-" snapshot-name)))))

(defn delete-snapshot
  "Delete an existing snapshot."
  [^Admin admin snapshot-name]
  (.deleteSnapshot admin snapshot-name))

(defn delete-snapshot-all
  "Delete snapshots created with 'snapshot-all'"
  [^Admin admin snapshot-name]
  (let [tables-name (list-tables-name-as-string admin)]
    (doseq [tn tables-name]
      (delete-snapshot admin (str tn "-" snapshot-name)))))

(defn restore-snapshot
  "Restore the specified snapshot on the original table."
  [^Admin admin snapshot-name]
  (.restoreSnapshot admin snapshot-name))

(defn clone-snapshot
  "Create a new table by cloning the snapshot content."
  [^Admin admin snapshot-name table-name]
  (.cloneSnapshot admin snapshot-name (TableName/valueOf table-name)))

(defn- mk-s3-url
  [with-creds? with-path? opts]
  (if with-creds?
    (str (:s3-protocol opts) (:access-key opts) ":" (:secret-key opts) + "@" (:bucket opts) (when with-path? (:path opts)))
    (str (:s3-protocol opts) (:bucket opts) (when with-path? (:path opts)))))

(defn- mk-toolrunner-args
  [{:keys [snapshot-name url-in url-out parallelism]}]
  (into-array
    (remove nil?
    ["-snaspshot"
     snapshot-name
     (when url-in "-copy-from")
     (when url-in url-in)
     "-copy-to"
     url-out
     "-mappers"
     (if (integer? parallelism) (.toString parallelism) parallelism)])))

 (defn- mk-toolrunner-import-config
   [conf opts]
   (let [tr-config (HBaseConfiguration/create conf)
         properties
    {:fs.default.name (mk-s3-url true false opts)
     :fs.defaultFS (mk-s3-url true false opts)
     :fs.s3.awsAccessKeyId (:access-key opts)
     :fs.s3.awsSecretAccessKey (:secret-key opts)
     :hbase.tmp.dir "/tmp/hbase-${user.name}"
     :hbase.rootdir (mk-s3-url true true opts)}]
     (update-hbase-config tr-config properties)))

(defn export-snapshot-to-s3
  "Export a single snapshot to s3"
  [hbase-name snapshot-name opts]
  (try
  (ToolRunner/run
    (get-config hbase-name)
    (ExportSnapshot.)
    (mk-toolrunner-args {:snapshot-name snapshot-name
                         :url-out (mk-s3-url true true opts)
                         :parallelism (or (:parallelism opts) 1)}))
    (catch Exception e
      (log/error "Exception occured while exporting snapshot to s3" e))))


(defn export-all-table-snapshot-to-s3
  "Export an 'all-table' snapshot to s3"
  [hbase-name snapshot-name opts]
  (try
    (let [admin (get-admin (get-connection hbase-name))
          tables-name (list-tables-name-as-string admin)]
          (doseq [tn tables-name]
            (export-snapshot-to-s3 admin (str tn "-" snapshot-name) opts)))
    (catch Exception e
      (log/error "Exception occured while exporting snapshot to s3" e))))

(defn import-snapshot-from-s3
  "Import a snapshot from s3 given a snapshot name"
  [hbase-name snapshot-name opts]
  (let [^HBaseConfiguration config (get-config hbase-name)
        tr-config (mk-toolrunner-import-config config opts)
        hdfsurl (or (.get config "fs.default.name") (.get config "fs.defaultFS"))]
    (if hdfsurl
    (try
    (ToolRunner/run
      tr-config
      (ExportSnapshot.)
      (mk-toolrunner-args {:snapshot-name snapshot-name
                           :url-in (mk-s3-url opts)
                           :url-out hdfsurl
                           :parallelism (or (:parallelism opts) 1)}))
          (catch Exception e
            (log/error "Exception occured while importing from s3")))
      (do
        (log/error "Missing hdfs url in config, can't import snapshot")
        {:error true :msg "missing hdfs url in config"}))))
