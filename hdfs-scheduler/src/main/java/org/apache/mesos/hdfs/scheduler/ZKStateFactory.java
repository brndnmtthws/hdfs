package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.state.State;
import org.apache.mesos.state.ZooKeeperState;

import java.util.concurrent.TimeUnit;

/**
 * Generates Zookeeper Mesos State abstractions.
 */
public class ZKStateFactory implements StateFactory {

  @Inject
  public State create(String path, HdfsFrameworkConfig config) {
    return new ZooKeeperState(
      config.getStateZkServers(),
      config.getStateZkTimeout(),
      TimeUnit.MILLISECONDS,
      path);
  }
}
