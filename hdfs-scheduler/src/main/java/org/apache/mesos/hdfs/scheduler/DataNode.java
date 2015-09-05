package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.LiveState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.Protos.Offer;

import java.util.Arrays;
import java.util.List;

/**
 * DataNode.
 */
public class DataNode extends HdfsNode {
  private final Log log = LogFactory.getLog(DataNode.class);

  public DataNode(LiveState liveState, IPersistentStateStore persistentStore, HdfsFrameworkConfig config) {
    super(liveState, persistentStore, config, HDFSConstants.DATA_NODE_ID);
  }

  public boolean evaluate(Offer offer) {
    boolean accept = false;

    if (offerNotEnoughResources(offer, config.getDataNodeCpus(), config.getDataNodeHeapSize())) {
      log.info("Offer does not have enough resources");
    } else {
      List<String> deadDataNodes = persistenceStore.getDeadDataNodes();
      // TODO (elingg) Relax this constraint to only wait for DN's when the number of DN's is small
      // What number of DN's should we try to recover or should we remove this constraint
      // entirely?
      if (deadDataNodes.isEmpty()) {
        if (persistenceStore.dataNodeRunningOnSlave(offer.getHostname())
            || persistenceStore.nameNodeRunningOnSlave(offer.getHostname())
            || persistenceStore.journalNodeRunningOnSlave(offer.getHostname())) {
          log.info(String.format("Already running hdfs task on %s", offer.getHostname()));
        } else {
          accept = true;
        }
      } else if (deadDataNodes.contains(offer.getHostname())) {
        accept = true;
      }
    }

    return accept;
  }

  protected String getExecutorName() {
    return HDFSConstants.NODE_EXECUTOR_ID;
  }

  protected List<String> getTaskTypes() {
    return Arrays.asList(HDFSConstants.DATA_NODE_ID);
  }
}
