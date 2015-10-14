package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.state.State;

/**
 * StateFactory Inteface.
 */
public interface StateFactory {
  public State create(String path, HdfsFrameworkConfig config);
}
