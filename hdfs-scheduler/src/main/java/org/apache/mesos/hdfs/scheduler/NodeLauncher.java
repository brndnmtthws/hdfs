package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.SchedulerDriver;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Attempts to launch HDFS nodes, after determining whether an offer is appropriate.
 */
public class NodeLauncher {
  private static final Log log = LogFactory.getLog(NodeLauncher.class);

  public boolean tryLaunch(SchedulerDriver driver, Offer offer, HdfsNode node)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {

    String nodeName = node.getName();
    OfferID offerId = offer.getId();

    log.info(String.format("Node: %s, evaluating offer: %s", nodeName, offerId));
    boolean acceptOffer = node.evaluate(offer);

    if (acceptOffer) {
      log.info(String.format("Node: %s, accepting offer: %s", nodeName, offerId));
      node.launch(driver, offer);
    } else {
      log.info(String.format("Node: %s, declining offer: %s", nodeName, offerId));
      driver.declineOffer(offerId);
    }

    return acceptOffer;
  }
}
