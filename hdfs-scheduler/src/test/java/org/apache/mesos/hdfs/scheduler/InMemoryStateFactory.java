package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.state.InMemoryState;
import org.apache.mesos.state.State;

/**
 * Generates in-memory Mesos State abstractions.
 */
public class InMemoryStateFactory implements StateFactory {

  @Inject
  public State create(String path, HdfsFrameworkConfig config) {
    return new InMemoryState();
  }
}
