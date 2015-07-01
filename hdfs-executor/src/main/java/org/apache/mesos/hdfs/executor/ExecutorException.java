package org.apache.mesos.hdfs.executor;

/**
 * A invalid condition exist within the executor
 *
 * @author kensipe
 */
public class ExecutorException extends RuntimeException {
  public ExecutorException() {
  }

  public ExecutorException(String message) {
    super(message);
  }
}
