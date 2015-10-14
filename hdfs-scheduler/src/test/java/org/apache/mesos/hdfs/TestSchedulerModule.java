package org.apache.mesos.hdfs;

import com.google.inject.AbstractModule;
import org.apache.mesos.hdfs.scheduler.InMemoryStateFactory;
import org.apache.mesos.hdfs.scheduler.StateFactory;

/**
 * Guice Module for initializing interfaces to implementations for the HDFS Scheduler.
 */
public class TestSchedulerModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(StateFactory.class).to(InMemoryStateFactory.class);
  }
}
