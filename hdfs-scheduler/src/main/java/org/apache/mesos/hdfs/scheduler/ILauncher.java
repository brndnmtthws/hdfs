package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Offer;
import org.apache.mesos.SchedulerDriver;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * ILauncher interface.
 */
public interface ILauncher {
  public void launch(SchedulerDriver driver, Offer offer)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException;
}
