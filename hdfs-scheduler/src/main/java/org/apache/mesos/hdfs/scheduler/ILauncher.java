package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.SchedulerDriver;

/**
 * ILauncher interface.
 */
public interface ILauncher {
  public boolean tryLaunch(SchedulerDriver driver, Offer offer);
}
