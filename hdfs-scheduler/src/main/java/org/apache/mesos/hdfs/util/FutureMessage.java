package org.apache.mesos.hdfs.util;

import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.scheduler.Scheduler;

/**
 * Used with timers to send a message in the future.  It is immutable.
 */
public class FutureMessage {

  private final Scheduler scheduler;
  private final SchedulerDriver driver;
  private final Protos.TaskID taskId;
  private final Protos.SlaveID slaveID;
  private final String message;


  public FutureMessage(SchedulerDriver driver, Scheduler scheduler, Protos.TaskID taskId, Protos.SlaveID slaveID, String message) {
    this.driver = driver;
    this.scheduler = scheduler;
    this.taskId = taskId;
    this.slaveID = slaveID;
    this.message = message;
  }

  public void send() {
    scheduler.sendMessageTo(driver, taskId, slaveID, message);
  }
}
