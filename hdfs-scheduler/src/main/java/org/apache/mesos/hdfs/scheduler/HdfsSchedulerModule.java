package org.apache.mesos.hdfs.scheduler;

import com.google.inject.AbstractModule;
import org.apache.mesos.hdfs.state.IPersistentStateStore;
import org.apache.mesos.hdfs.state.PersistentStateStore;

/**
 * Guice Module for initializing interfaces to implementations for the HDFS Scheduler.
 */
public class HdfsSchedulerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(IPersistentStateStore.class).to(PersistentStateStore.class);
  }
}
