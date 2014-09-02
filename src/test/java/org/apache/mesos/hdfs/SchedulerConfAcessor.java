package org.apache.mesos.hdfs;

import com.google.inject.Inject;
import org.apache.mesos.hdfs.config.SchedulerConf;

public class SchedulerConfAcessor {
  private SchedulerConf schedulerConf;

  @Inject
  SchedulerConfAcessor(SchedulerConf schedulerConf) {
    this.schedulerConf = schedulerConf;
  }

  public SchedulerConf getSchedulerConf() {
    return schedulerConf;
  }
}
