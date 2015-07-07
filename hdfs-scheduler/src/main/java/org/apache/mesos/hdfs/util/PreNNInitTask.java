package org.apache.mesos.hdfs.util;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.scheduler.Scheduler;

import java.util.TimerTask;

/**
 * Used for a NameNode init timer to see if DNS is complete
 */
public class PreNNInitTask extends TimerTask {

  private final DnsResolver dnsResolver;
  private final Scheduler scheduler;
  private final SchedulerDriver driver;
  private final Protos.TaskID taskId;
  private final Protos.SlaveID slaveID;
  private final String message;

  public PreNNInitTask(DnsResolver dnsResolver, Scheduler scheduler, SchedulerDriver driver, Protos.TaskID taskId,
    Protos.SlaveID slaveID, String message) {
    this.dnsResolver = dnsResolver;
    this.scheduler = scheduler;
    this.driver = driver;
    this.taskId = taskId;
    this.slaveID = slaveID;
    this.message = message;
  }

  @Override
  public void run() {
    if (dnsResolver.nameNodesResolvable()) {
      scheduler.sendMessageTo(driver, taskId, slaveID, message);
      this.cancel();
    }
  }
}
