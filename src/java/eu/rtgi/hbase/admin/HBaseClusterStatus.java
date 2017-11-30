package eu.rtgi.hbase.admin;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Collection;

import clojure.lang.PersistentHashMap;
import clojure.lang.ITransientMap;
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
    ITransientMap itmap = PersistentHashMap.EMPTY.asTransient();
    itmap.assoc(kw("host"),server.getHostname());
    itmap.assoc(kw("port"),server.getPort());
    return (PersistentHashMap) itmap.persistent();
  }

  private static PersistentVector getServerList(Collection<ServerName> slist){
    ArrayList<Object> sL = new ArrayList<Object>();
    for (ServerName server : slist){
      sL.add(getServerMap(server));
    }
    return PersistentVector.create(sL);
  }

  private static PersistentHashMap getRegionInfoMap(HRegionInfo rIn){
    ITransientMap itmap = PersistentHashMap.EMPTY.asTransient();
    itmap.assoc(kw("encoded-name"),rIn.getEncodedName());
    itmap.assoc(kw("region-name"),rIn.getRegionNameAsString());
    itmap.assoc(kw("table-name"),rIn.getTable().getNameAsString());
    itmap.assoc(kw("table-namespace"),rIn.getTable().getNamespaceAsString());
    return (PersistentHashMap) itmap.persistent();
  }


  private static PersistentHashMap getRegionStateMap(RegionState rSt){
    ITransientMap itmap = PersistentHashMap.EMPTY.asTransient();
    PersistentHashMap sm = null;
    if(rSt.getServerName() != null){
      sm = getServerMap(rSt.getServerName());
    }
    itmap.assoc(kw("server"),sm);
    itmap.assoc(kw("is-closed"),rSt.isClosed());
    itmap.assoc(kw("is-closing"),rSt.isClosing());
    itmap.assoc(kw("is-failed-close"),rSt.isFailedClose());
    itmap.assoc(kw("is-failed-open"),rSt.isFailedOpen());
    itmap.assoc(kw("is-merged"),rSt.isMerged());
    itmap.assoc(kw("is-merging"),rSt.isMerging());
    itmap.assoc(kw("is-opened"),rSt.isOpened());
    itmap.assoc(kw("is-split"),rSt.isSplit());
    itmap.assoc(kw("is-splitting"),rSt.isSplitting());
    itmap.assoc(kw("is-offline"),rSt.isOffline());
    itmap.assoc(kw("state"),rSt.getState().toString());
    itmap.assoc(kw("region-info"),getRegionInfoMap(rSt.getRegion()));
    return (PersistentHashMap) itmap.persistent();
  }

  private static PersistentVector getRegionStateList(List<RegionState> rStlist){
    ArrayList<Object> sL = new ArrayList<Object>();
    for (RegionState regionState : rStlist){
      sL.add(getRegionStateMap(regionState));
    }
    return PersistentVector.create(sL);
  }

  public static PersistentHashMap get(Admin admin) throws IOException, InterruptedException{
    ClusterStatus st = admin.getClusterStatus();
    ITransientMap itmap = PersistentHashMap.EMPTY.asTransient();
    itmap.assoc(kw("hbase-version"),st.getHBaseVersion());
    itmap.assoc(kw("version"),st.getVersion());
    itmap.assoc(kw("cluster-id"),st.getClusterId());
    itmap.assoc(kw("master"),getServerMap(st.getMaster()));
    itmap.assoc(kw("nb-master-backup"),st.getBackupMastersSize());
    itmap.assoc(kw("backup-masters"),getServerList(st.getBackupMasters()));
    itmap.assoc(kw("nb-live-servers"),st.getServersSize());
    itmap.assoc(kw("live-servers"),getServerList(st.getServers()));
    itmap.assoc(kw("nb-dead-servers"),st.getDeadServersSize());
    itmap.assoc(kw("dead-servers"),getServerList(st.getDeadServerNames()));
    itmap.assoc(kw("nb-regions"),st.getRegionsCount());
    itmap.assoc(kw("regions-in-transitions"),getRegionStateList(st.getRegionsInTransition()));
    itmap.assoc(kw("nb-requests"),st.getRequestsCount());
    itmap.assoc(kw("is-balancer-on"),st.isBalancerOn());
    itmap.assoc(kw("average-load"),st.getAverageLoad());
    return (PersistentHashMap) itmap.persistent();
  }

  public static PersistentHashMap getRegionLoadMap(RegionLoad rl){
    ITransientMap itmap = PersistentHashMap.EMPTY.asTransient();
    itmap.assoc(kw("name-as-string"),rl.getNameAsString());
    itmap.assoc(kw("nb-requests"),rl.getRequestsCount());
    itmap.assoc(kw("nb-read-requests"),rl.getReadRequestsCount());
    itmap.assoc(kw("nb-write-requests"),rl.getWriteRequestsCount());
    itmap.assoc(kw("nb-stores"),rl.getStores());
    itmap.assoc(kw("nb-stores-files"),rl.getStorefiles());
    itmap.assoc(kw("data-locality"),rl.getDataLocality());
    itmap.assoc(kw("store-file-size-mb"),rl.getStorefileSizeMB());
    itmap.assoc(kw("store-file-index-size-mb"),rl.getStorefileIndexSizeMB());
    itmap.assoc(kw("memstore-size-mb"),rl.getMemStoreSizeMB());
    itmap.assoc(kw("root-index-size-kb"),rl.getRootIndexSizeKB());
    itmap.assoc(kw("total-static-bloom-size-kb"),rl.getTotalStaticBloomSizeKB());
    itmap.assoc(kw("total-static-index-size-kb"),rl.getTotalStaticIndexSizeKB());
    itmap.assoc(kw("current-compacted-kvs"),rl.getCurrentCompactedKVs());
    itmap.assoc(kw("total-compacting-kvs"),rl.getTotalCompactingKVs());
    return (PersistentHashMap) itmap.persistent();
  }

  public static PersistentVector getRegionsLoadPV(Map<byte[],RegionLoad> rls){
    ArrayList<Object> rlL = new ArrayList<Object>();
    for (Map.Entry<byte[], RegionLoad> entry : rls.entrySet()) {
      rlL.add(getRegionLoadMap(entry.getValue()));
    }
    return PersistentVector.create(rlL);
  }

  public static PersistentHashMap getServerLoad(ClusterStatus st, ServerName server) throws IOException, InterruptedException{
    ITransientMap itmap = PersistentHashMap.EMPTY.asTransient();
    ITransientMap itmapl = PersistentHashMap.EMPTY.asTransient();
    itmap.assoc(kw("server"),getServerMap(server));
    ServerLoad load = st.getLoad(server);
    itmapl.assoc(kw("nb-regions"),load.getNumberOfRegions());
    itmapl.assoc(kw("nb-requests"),load.getNumberOfRequests());
    itmapl.assoc(kw("nb-read-requests"),load.getReadRequestsCount());
    itmapl.assoc(kw("nb-write-requests"),load.getWriteRequestsCount());
    itmapl.assoc(kw("memstore-size-mb"),load.getMemstoreSizeInMB());
    itmapl.assoc(kw("requests-per-second"),load.getRequestsPerSecond());
    itmapl.assoc(kw("nb-stores"),load.getStores());
    itmapl.assoc(kw("uncompressed-store-size-mb"),load.getStoreUncompressedSizeMB());
    itmapl.assoc(kw("nb-store-files"),load.getStorefiles());
    itmapl.assoc(kw("store-file-size-mb"),load.getStorefileSizeInMB());
    itmapl.assoc(kw("store-file-index-size-mb"),load.getStorefileIndexSizeInMB());
    itmapl.assoc(kw("root-index-size-kb"),load.getRootIndexSizeKB());
    itmapl.assoc(kw("total-static-bloom-size-kb"),load.getTotalStaticBloomSizeKB());
    itmapl.assoc(kw("total-static-index-size-kb"),load.getTotalStaticIndexSizeKB());
    itmapl.assoc(kw("current-compacted-kvs"),load.getCurrentCompactedKVs());
    itmapl.assoc(kw("total-compacting-kvs"),load.getTotalCompactingKVs());
    itmapl.assoc(kw("rs-coprocessor"),PersistentVector.adopt((Object[]) load.getRegionServerCoprocessors()));
    itmapl.assoc(kw("rs-coprocessor-region-level"),PersistentVector.adopt((Object[]) load.getRsCoprocessors()));
    itmapl.assoc(kw("max-heap-mb"),load.getMaxHeapMB());
    itmapl.assoc(kw("used-heap-mb"),load.getUsedHeapMB());
    itmap.assoc(kw("load-info"),(PersistentHashMap) itmapl.persistent());
    itmap.assoc(kw("regions-load"),getRegionsLoadPV(load.getRegionsLoad()));
    return (PersistentHashMap) itmap.persistent();
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
