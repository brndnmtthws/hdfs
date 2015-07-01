package org.apache.mesos.hdfs.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TimerTask;

/**
 * Used for a NameNode init timer to see if DNS is complete
 */
public class PreNNInitTask extends TimerTask {

  private final Logger logger = LoggerFactory.getLogger(getClass());
  private DnsResolver dnsResolver;
  private FutureMessage futureMessage;

  public PreNNInitTask(FutureMessage futureMessage, DnsResolver dnsResolver) {
    this.futureMessage = futureMessage;
    this.dnsResolver = dnsResolver;
  }

  @Override
  public void run() {
    if (dnsResolver.nameNodesResolvable()) {
      this.cancel();
      futureMessage.send();
    }
  }
}
