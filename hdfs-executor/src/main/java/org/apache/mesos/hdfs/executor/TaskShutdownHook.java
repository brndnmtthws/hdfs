package org.apache.mesos.hdfs.executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;

/**
 */
public class TaskShutdownHook implements Runnable {

  private final Log log = LogFactory.getLog(TaskShutdownHook.class);

  private Executor executor;
  private ExecutorDriver driver;

  public TaskShutdownHook(Executor executor, ExecutorDriver driver) {
    this.executor = executor;
    this.driver = driver;
  }

  @Override
  public void run() {
    log.info("shutdown hook shutting down tasks");
    executor.shutdown(this.driver);
  }
}
