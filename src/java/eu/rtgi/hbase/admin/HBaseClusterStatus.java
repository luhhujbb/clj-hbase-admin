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
    cSL.add(st.getDeadServerNames().size());
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

  public static PersistentHashMap getRegionLoadMap(RegionLoad rl){
    ArrayList<Object> rlPL = new ArrayList<Object>();
    rlPL.add(kw("name-as-string"));
    rlPL.add(rl.getNameAsString());
    rlPL.add(kw("nb-requests"));
    rlPL.add(rl.getRequestsCount());
    rlPL.add(kw("nb-read-requests"));
    rlPL.add(rl.getReadRequestsCount());
    rlPL.add(kw("nb-write-requests"));
    rlPL.add(rl.getWriteRequestsCount());
    rlPL.add(kw("nb-stores"));
    rlPL.add(rl.getStores());
    rlPL.add(kw("nb-stores-files"));
    rlPL.add(rl.getStorefiles());
    rlPL.add(kw("data-locality"));
    rlPL.add(rl.getDataLocality());
    rlPL.add(kw("store-file-size-mb"));
    rlPL.add(rl.getStorefileSizeMB());
    rlPL.add(kw("store-file-index-size-mb"));
    rlPL.add(rl.getStorefileIndexSizeMB());
    rlPL.add(kw("memstore-size-mb"));
    rlPL.add(rl.getMemStoreSizeMB());
    rlPL.add(kw("root-index-size-kb"));
    rlPL.add(rl.getRootIndexSizeKB());
    rlPL.add(kw("total-static-bloom-size-kb"));
    rlPL.add(rl.getTotalStaticBloomSizeKB());
    rlPL.add(kw("total-static-index-size-kb"));
    rlPL.add(rl.getTotalStaticIndexSizeKB());
    rlPL.add(kw("current-compacted-kvs"));
    rlPL.add(rl.getCurrentCompactedKVs());
    rlPL.add(kw("total-compacting-kvs"));
    rlPL.add(rl.getTotalCompactingKVs());
    return map(rlPL);
  }

  public static PersistentVector getRegionsLoadPV(Map<byte[],RegionLoad> rls){
    ArrayList<Object> rlL = new ArrayList<Object>();
    for (Map.Entry<byte[], RegionLoad> entry : rls.entrySet()) {
      rlL.add(getRegionLoadMap(entry.getValue()));
    }
    return PersistentVector.create(rlL);
  }

  public static PersistentHashMap getServerLoad(ClusterStatus st, ServerName server) throws IOException, InterruptedException{
    ArrayList<Object> sLPL = new ArrayList<Object>();
    ArrayList<Object> sLIPL = new ArrayList<Object>();
    sLPL.add(kw("server"));
    sLPL.add(getServerMap(server));
    ServerLoad load = st.getLoad(server);
    sLPL.add(kw("load-info"));
    sLIPL.add(kw("nb-regions"));
    sLIPL.add(load.getNumberOfRegions());
    sLIPL.add(kw("nb-requests"));
    sLIPL.add(load.getNumberOfRequests());
    sLIPL.add(kw("nb-read-requests"));
    sLIPL.add(load.getReadRequestsCount());
    sLIPL.add(kw("nb-write-requests"));
    sLIPL.add(load.getWriteRequestsCount());
    sLIPL.add(kw("memstore-size-mb"));
    sLIPL.add(load.getMemstoreSizeInMB());
    sLIPL.add(kw("requests-per-second"));
    sLIPL.add(load.getRequestsPerSecond());
    sLIPL.add(kw("nb-stores"));
    sLIPL.add(load.getStores());
    sLIPL.add(kw("uncompressed-store-size-mb"));
    sLIPL.add(load.getStoreUncompressedSizeMB());
    sLIPL.add(kw("nb-store-files"));
    sLIPL.add(load.getStorefiles());
    sLIPL.add(kw("store-file-size-mb"));
    sLIPL.add(load.getStorefileSizeInMB());
    sLIPL.add(kw("store-file-index-size-mb"));
    sLIPL.add(load.getStorefileIndexSizeInMB());
    sLIPL.add(kw("root-index-size-kb"));
    sLIPL.add(load.getRootIndexSizeKB());
    sLIPL.add(kw("total-static-bloom-size-kb"));
    sLIPL.add(load.getTotalStaticBloomSizeKB());
    sLIPL.add(kw("total-static-index-size-kb"));
    sLIPL.add(load.getTotalStaticIndexSizeKB());
    sLIPL.add(kw("current-compacted-kvs"));
    sLIPL.add(load.getCurrentCompactedKVs());
    sLIPL.add(kw("total-compacting-kvs"));
    sLIPL.add(load.getTotalCompactingKVs());
    sLIPL.add(kw("rs-coprocessor"));
    sLIPL.add(PersistentVector.adopt((Object[]) load.getRegionServerCoprocessors()));
    sLIPL.add(kw("rs-coprocessor-region-level"));
    sLIPL.add(PersistentVector.adopt((Object[]) load.getRsCoprocessors()));
    sLIPL.add(kw("max-heap-mb"));
    sLIPL.add(load.getMaxHeapMB());
    sLIPL.add(kw("used-heap-mb"));
    sLIPL.add(load.getUsedHeapMB());
    sLPL.add(map(sLIPL));
    sLPL.add(kw("regions-load"));
    sLPL.add(getRegionsLoadPV(load.getRegionsLoad()));
    return map(sLPL);
  }

  public static PersistentVector getServersLoad(Admin admin) throws IOException, InterruptedException{
    ClusterStatus st = admin.getClusterStatus();
    ArrayList<Object> sLL = new ArrayList<Object>();
    for (ServerName server : st.getServers()) {
      sLL.add(getServerLoad(st,server));
    }
    return PersistentVector.create(sLL);
  }

}
