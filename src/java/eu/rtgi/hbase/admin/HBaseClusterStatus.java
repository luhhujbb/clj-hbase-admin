package eu.rtgi.hbase.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Collection;

import clojure.lang.PersistentHashMap;
import clojure.lang.PersistentList;
import clojure.lang.PersistentVector;
import clojure.lang.Keyword;
import clojure.lang.ISeq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

// cc ClusterStatusExample Example reporting the st of a cluster
public class HBaseClusterStatus {
  private static Keyword kw(String s){
    return Keyword.intern(s);
  }

  private static PersistentHashMap map(ArrayList<Object> seq){
    return PersistentHashMap.create((ISeq) PersistentList.create(seq));
  }

  private static PersistentHashMap getServerMap(ServerName server){
    ArrayList<Object> sPL = new ArrayList<Object>();
    sPL.add(kw("host"));
    sPL.add(server.getHostname());
    sPL.add(kw("port"));
    sPL.add(server.getPort());
    return map(sPL);
  }

  private static PersistentVector getServerList(Collection<ServerName> slist){
    ArrayList<Object> sL = new ArrayList<Object>();
    for (ServerName server : slist){
      sL.add(getServerMap(server));
    }
    return PersistentVector.create(sL);
  }

  private static PersistentHashMap getRegionInfoMap(HRegionInfo rIn){
    ArrayList<Object> rInL = new ArrayList<Object>();
    rInL.add(kw("encoded-name"));
    rInL.add(rIn.getEncodedName());
    rInL.add(kw("region-name"));
    rInL.add(rIn.getRegionNameAsString());
    rInL.add(kw("table-name"));
    rInL.add(rIn.getTable().getNameAsString());
    rInL.add(kw("table-namespace"));
    rInL.add(rIn.getTable().getNamespaceAsString());
    return map(rInL);
  }


  private static PersistentHashMap getRegionStateMap(RegionState rSt){
    ArrayList<Object> rStL = new ArrayList<Object>();
    PersistentHashMap sm = null;
    if(rSt.getServerName() != null){
      sm = getServerMap(rSt.getServerName());
    }
    rStL.add(kw("server"));
    rStL.add(sm);
    rStL.add(kw("is-closed"));
    rStL.add(rSt.isClosed());
    rStL.add(kw("is-closing"));
    rStL.add(rSt.isClosing());
    rStL.add(kw("is-failed-close"));
    rStL.add(rSt.isFailedClose());
    rStL.add(kw("is-failed-open"));
    rStL.add(rSt.isFailedOpen());
    rStL.add(kw("is-merged"));
    rStL.add(rSt.isMerged());
    rStL.add(kw("is-merging"));
    rStL.add(rSt.isMerging());
    rStL.add(kw("is-opened"));
    rStL.add(rSt.isOpened());
    rStL.add(kw("is-split"));
    rStL.add(rSt.isSplit());
    rStL.add(kw("is-splitting"));
    rStL.add(rSt.isSplitting());
    rStL.add(kw("is-offline"));
    rStL.add(rSt.isOffline());
    rStL.add(kw("state"));
    rStL.add(rSt.getState().toString());
    rStL.add(kw("region-info"));
    rStL.add(getRegionInfoMap(rSt.getRegion()));
    return map(rStL);
  }

  private static PersistentVector getRegionStateList(Map<String,RegionState> rStlist){
    ArrayList<Object> sL = new ArrayList<Object>();
    for (Map.Entry<String,RegionState> entry : rStlist.entrySet()){
      sL.add(getRegionStateMap(entry.getValue()));
    }
    return PersistentVector.create(sL);
  }

  public static PersistentHashMap get(Admin admin) throws IOException, InterruptedException{
    ClusterStatus st = admin.getClusterStatus();
    ArrayList<Object> cSL = new ArrayList<Object>();
    cSL.add(kw("hbase-version"));
    cSL.add(st.getHBaseVersion());
    cSL.add(kw("version"));
    cSL.add(st.getVersion());
    cSL.add(kw("cluster-id"));
    cSL.add(st.getClusterId());
    cSL.add(kw("master"));
    cSL.add(getServerMap(st.getMaster()));
    cSL.add(kw("nb-master-backup"));
    cSL.add(st.getBackupMastersSize());
    cSL.add(kw("backup-masters"));
    cSL.add(getServerList(st.getBackupMasters()));
    cSL.add(kw("nb-live-servers"));
    cSL.add(st.getServersSize());
    cSL.add(kw("live-servers"));
    cSL.add(getServerList(st.getServers()));
    cSL.add(kw("nb-dead-servers"));
    //this is deprecated in 2.0.0 branch
    //cSL.add(st.getDeadServersSize());
    cSL.add(st.getDeadServers());
    cSL.add(kw("dead-servers"));
    cSL.add(getServerList(st.getDeadServerNames()));
    cSL.add(kw("nb-regions"));
    cSL.add(st.getRegionsCount());
    cSL.add(kw("regions-in-transitions"));
    cSL.add(getRegionStateList(st.getRegionsInTransition()));
    cSL.add(kw("nb-requests"));
    cSL.add(st.getRequestsCount());
    cSL.add(kw("is-balancer-on"));
    cSL.add(st.isBalancerOn());
    cSL.add(kw("average-load"));
    cSL.add(st.getAverageLoad());
    return map(cSL);
  }
}

  //public PersistentHashMap getServersLoad(){

  //}

  //public PersistentHashMap getRegionsLoad(){

  //}



//     System.out.println("\nServer Info:\n--------------");
//     for (ServerName server : st.getServers()) { // co ClusterStatusExample-2-ServerInfo Iterate over the included server instances.
//       System.out.println("Hostname: " + server.getHostname());
//       System.out.println("Host and Port: " + server.getHostAndPort());
//       System.out.println("Server Name: " + server.getServerName());
//       System.out.println("RPC Port: " + server.getPort());
//       System.out.println("Start Code: " + server.getStartcode());
//
//       ServerLoad load = st.getLoad(server); // co ClusterStatusExample-3-ServerLoad Retrieve the load details for the current server.
//
//       System.out.println("\nServer Load:\n--------------");
//       System.out.println("Info Port: " + load.getInfoServerPort());
//       System.out.println("Load: " + load.getLoad());
//       System.out.println("Max Heap (MB): " + load.getMaxHeapMB());
//       System.out.println("Used Heap (MB): " + load.getUsedHeapMB());
//       System.out.println("Memstore Size (MB): " +
//         load.getMemstoreSizeInMB());
//       System.out.println("No. Regions: " + load.getNumberOfRegions());
//       System.out.println("No. Requests: " + load.getNumberOfRequests());
//       System.out.println("Total No. Requests: " +
//         load.getTotalNumberOfRequests());
//       System.out.println("No. Requests per Sec: " +
//         load.getRequestsPerSecond());
//       System.out.println("No. Read Requests: " +
//         load.getReadRequestsCount());
//       System.out.println("No. Write Requests: " +
//         load.getWriteRequestsCount());
//       System.out.println("No. Stores: " + load.getStores());
//       System.out.println("Store Size Uncompressed (MB): " +
//         load.getStoreUncompressedSizeMB());
//       System.out.println("No. Storefiles: " + load.getStorefiles());
//       System.out.println("Storefile Size (MB): " +
//         load.getStorefileSizeInMB());
//       System.out.println("Storefile Index Size (MB): " +
//         load.getStorefileIndexSizeInMB());
//       System.out.println("Root Index Size: " + load.getRootIndexSizeKB());
//       System.out.println("Total Bloom Size: " +
//         load.getTotalStaticBloomSizeKB());
//       System.out.println("Total Index Size: " +
//         load.getTotalStaticIndexSizeKB());
//       System.out.println("Current Compacted Cells: " +
//         load.getCurrentCompactedKVs());
//       System.out.println("Total Compacting Cells: " +
//         load.getTotalCompactingKVs());
//       System.out.println("Coprocessors1: " +
//         Arrays.asList(load.getRegionServerCoprocessors()));
//       System.out.println("Coprocessors2: " +
//         Arrays.asList(load.getRsCoprocessors()));
//       System.out.println("Replication Load Sink: " +
//         load.getReplicationLoadSink());
//       System.out.println("Replication Load Source: " +
//         load.getReplicationLoadSourceList());
//
//       System.out.println("\nRegion Load:\n--------------");
//       for (Map.Entry<byte[], RegionLoad> entry : // co ClusterStatusExample-4-Regions Iterate over the region details of the current server.
//           load.getRegionsLoad().entrySet()) {
//         System.out.println("Region: " + Bytes.toStringBinary(entry.getKey()));
//
//         RegionLoad regionLoad = entry.getValue(); // co ClusterStatusExample-5-RegionLoad Get the load details for the current region.
//
//         System.out.println("Name: " + Bytes.toStringBinary(
//           regionLoad.getName()));
//         System.out.println("Name (as String): " +
//           regionLoad.getNameAsString());
//         System.out.println("No. Requests: " + regionLoad.getRequestsCount());
//         System.out.println("No. Read Requests: " +
//           regionLoad.getReadRequestsCount());
//         System.out.println("No. Write Requests: " +
//           regionLoad.getWriteRequestsCount());
//         System.out.println("No. Stores: " + regionLoad.getStores());
//         System.out.println("No. Storefiles: " + regionLoad.getStorefiles());
//         System.out.println("Data Locality: " + regionLoad.getDataLocality());
//         System.out.println("Storefile Size (MB): " +
//           regionLoad.getStorefileSizeMB());
//         System.out.println("Storefile Index Size (MB): " +
//           regionLoad.getStorefileIndexSizeMB());
//         System.out.println("Memstore Size (MB): " +
//           regionLoad.getMemStoreSizeMB());
//         System.out.println("Root Index Size: " +
//           regionLoad.getRootIndexSizeKB());
//         System.out.println("Total Bloom Size: " +
//           regionLoad.getTotalStaticBloomSizeKB());
//         System.out.println("Total Index Size: " +
//           regionLoad.getTotalStaticIndexSizeKB());
//         System.out.println("Current Compacted Cells: " +
//           regionLoad.getCurrentCompactedKVs());
//         System.out.println("Total Compacting Cells: " +
//           regionLoad.getTotalCompactingKVs());
//         System.out.println();
//       }
//     }
//     // ^^ ClusterStatusExample
//   }
// }
