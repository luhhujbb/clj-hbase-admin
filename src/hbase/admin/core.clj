(ns hbase.admin.core
  (:require [clojure.java [io :as io]])
  (:import [java.io.InputStream]
           [org.apache.hadoop.conf Configuration Configured]
           [org.apache.hadoop.hbase HBaseConfiguration]
           [org.apache.hadoop.hbase.client ConnectionFactory Admin]
           [org.apache.hadoop.hbase.protobuf.generated HBaseProtos$SnapshotDescription]
           [org.apache.hadoop.hbase.snapshot ExportSnapshot	SnapshotCreationException]))

(def hbase-connection-registry (atom {}))

(defmulti mk-hbase-config
            (fn [x] (cond
              (instance? java.io.InputStream x) :input-stream
              (instance? clojure.lang.PersistentArrayMap x) :map
              :else :not-supported)))

(defmethod mk-hbase-config :map [properties])

(defmethod mk-hbase-config :input-stream [input-stream])

(defn mk-hbase-connection
  [quorum base-path])
