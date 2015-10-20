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
 * JournalNode.
 */
public class JournalNode extends HdfsNode {
  private final Log log = LogFactory.getLog(JournalNode.class);

  public JournalNode(
    HdfsState state,
    HdfsFrameworkConfig config) {
    super(state, config, HDFSConstants.JOURNAL_NODE_ID);
  }

  public boolean evaluate(Offer offer) {
    boolean accept = false;

    NodeConfig journalNodeConfig = config.getNodeConfig(HDFSConstants.JOURNAL_NODE_ID);

    int journalCount = 0;
    try {
      journalCount = state.getJournalCount();
    } catch (Exception ex) {
      log.error("Failed to retrieve Journal count with exception: " + ex);
      return false;
    }

    if (!enoughResources(offer, journalNodeConfig.getCpus(), journalNodeConfig.getMaxHeap())) {
      log.info("Offer does not have enough resources");
    } else if (journalCount >= config.getJournalNodeCount()) {
      log.info(String.format("Already running %s journalnodes", config.getJournalNodeCount()));
    } else if (state.hostOccupied(offer.getHostname(), HDFSConstants.JOURNAL_NODE_ID)) {
      log.info(String.format("Already running journalnode on %s", offer.getHostname()));
    } else if (config.getRunDatanodeExclusively() && state.hostOccupied(offer.getHostname(), HDFSConstants.DATA_NODE_ID)) {
      log.info(String.format("Cannot colocate journalnode and datanode on %s", offer.getHostname()));
    } else {
      accept = true;
    }

    return accept;
  }

  protected String getExecutorName() {
    return HDFSConstants.NODE_EXECUTOR_ID;
  }

  protected List<String> getTaskTypes() {
    return Arrays.asList(HDFSConstants.JOURNAL_NODE_ID);
  }
}
