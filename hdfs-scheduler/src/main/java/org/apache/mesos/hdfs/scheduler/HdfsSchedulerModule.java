package org.apache.mesos.hdfs.scheduler;

import com.google.inject.AbstractModule;
import org.apache.mesos.hdfs.state.PersistenceManager;
import org.apache.mesos.hdfs.state.PersistenceManagerImpl;

/**
 * Guice Module for initializing interfaces to implementations for the HDFS Scheduler.
 */
public class HdfsSchedulerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(PersistenceManager.class).to(PersistenceManagerImpl.class);
  }
}
