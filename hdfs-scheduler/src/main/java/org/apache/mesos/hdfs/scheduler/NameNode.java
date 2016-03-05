package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.util.DnsResolver;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.Arrays;
import java.util.List;

/**
 * Namenode.
 */
public class NameNode extends HdfsNode {
  private final Log log = LogFactory.getLog(NameNode.class);
  private String executorName = HDFSConstants.NAME_NODE_EXECUTOR_ID;
  private DnsResolver dnsResolver;

  public NameNode(
    HdfsState state,
    DnsResolver dnsResolver,
    HdfsFrameworkConfig config) {
    super(state, config, HDFSConstants.NAME_NODE_ID);
    this.dnsResolver = dnsResolver;
  }

  public boolean evaluate(Offer offer) {
    boolean accept = false;
    String hostname = offer.getHostname();

    if (dnsResolver.journalNodesResolvable()) {
      NodeConfig nameNodeConfig = config.getNodeConfig(HDFSConstants.NAME_NODE_ID);
      NodeConfig zkfcNodeConfig = config.getNodeConfig(HDFSConstants.ZKFC_NODE_ID);

      int nameCount = 0;
      try {
        nameCount = state.getNameCount();
      } catch (Exception ex) {
        log.error("Failed to retrieve NameNode count with exception: " + ex);
        return false;
      }

      if (!enoughResources(offer,
        (nameNodeConfig.getCpus() + zkfcNodeConfig.getCpus()),
        (nameNodeConfig.getMaxHeap() + zkfcNodeConfig.getMaxHeap()))) {
        log.info("Offer does not have enough resources");
      } else if (nameCount >= HDFSConstants.TOTAL_NAME_NODES) {
        log.info(String.format("Already running %s namenodes", HDFSConstants.TOTAL_NAME_NODES));
      } else if (state.hostOccupied(hostname, HDFSConstants.NAME_NODE_ID)) {
        log.info(String.format("Already running namenode on %s", offer.getHostname()));
      } else if (config.getRunDatanodeExclusively()
        && state.hostOccupied(hostname, HDFSConstants.DATA_NODE_ID)) {
        log.info(String.format("Cannot colocate namenode and datanode on %s", offer.getHostname()));
      } else if (!state.hostOccupied(hostname, HDFSConstants.JOURNAL_NODE_ID)) {
        log.info(String.format("We need to colocate the namenode with a journalnode and there is "
          + "no journalnode running on this host. %s", offer.getHostname()));
      } else {
        accept = true;
      }
    }

    return accept;
  }

  protected String getExecutorName() {
    return HDFSConstants.NAME_NODE_EXECUTOR_ID;
  }

  protected List<String> getTaskTypes() {
    return Arrays.asList(HDFSConstants.NAME_NODE_ID, HDFSConstants.ZKFC_NODE_ID);
  }
}
