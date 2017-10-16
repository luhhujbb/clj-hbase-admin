(defproject hbase-admin "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.clojure/tools.cli "0.3.1"]
                 [org.slf4j/slf4j-api "1.7.10"]
                 [org.slf4j/slf4j-log4j12 "1.7.10"]
                 [log4j "1.2.17"]

                 [org.apache.hadoop/hadoop-common "2.7.3"]
                 [org.apache.hadoop/hadoop-hdfs "2.7.3"]
                 [org.apache.hadoop/hadoop-client "2.7.3"]
                 [org.apache.hadoop/hadoop-mapreduce-client-core "2.7.3"]
                 [org.apache.hbase/hbase-client "1.1.3"]
                 [org.apache.hbase/hbase-server "1.1.3"]
                 [org.apache.hbase/hbase-protocol "1.1.3"]
                 [org.apache.hbase/hbase "1.1.3" :extension "pom"]
                 [amazonica "0.3.111"]]

  :aot :all)
