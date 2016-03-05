package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.Arrays;
import java.util.List;

/**
 * DataNode.
 */
public class DataNode extends HdfsNode {
  private final Log log = LogFactory.getLog(DataNode.class);

  public DataNode(
    HdfsState state,
    HdfsFrameworkConfig config) {
    super(state, config, HDFSConstants.DATA_NODE_ID);
  }

  public boolean evaluate(Offer offer) {
    boolean accept = false;
    NodeConfig dataNodeConfig = config.getNodeConfig(HDFSConstants.DATA_NODE_ID);

    if (!enoughResources(offer, dataNodeConfig.getCpus(), dataNodeConfig.getMaxHeap())) {
      log.info("Offer does not have enough resources");
    } else if (state.hostOccupied(offer.getHostname(), HDFSConstants.DATA_NODE_ID)) {
      log.info(String.format("Already running DataNode on %s", offer.getHostname()));
    } else if (violatesExclusivityConstraint(offer)) {
      log.info(String.format("Already running NameNode or JournalNode on %s", offer.getHostname()));
    } else {
      accept = true;
    }

    return accept;
  }

  protected String getExecutorName() {
    return HDFSConstants.NODE_EXECUTOR_ID;
  }

  protected List<String> getTaskTypes() {
    return Arrays.asList(HDFSConstants.DATA_NODE_ID);
  }

  private boolean violatesExclusivityConstraint(Offer offer) {
    return config.getRunDatanodeExclusively() &&
      (state.hostOccupied(offer.getHostname(), HDFSConstants.NAME_NODE_ID)
        || state.hostOccupied(offer.getHostname(), HDFSConstants.JOURNAL_NODE_ID));
  }
}
