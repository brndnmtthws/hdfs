package org.apache.mesos.hdfs.scheduler;

import com.google.inject.AbstractModule;

/**
 * Guice Module for initializing interfaces to implementations for the HDFS Scheduler.
 */
public class HdfsSchedulerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StateFactory.class).to(ZKStateFactory.class);
  }
}
