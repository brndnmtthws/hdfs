package org.apache.mesos.hdfs.state;

/**
 * Exception for issues with storing or retrieving state.
 */
public class PersistenceException extends RuntimeException {

  public PersistenceException(Throwable cause) {
    super(cause);
  }

  public PersistenceException(String message) {
    super(message);
  }

  public PersistenceException(String message, Throwable cause) {
    super(message, cause);
  }
}
