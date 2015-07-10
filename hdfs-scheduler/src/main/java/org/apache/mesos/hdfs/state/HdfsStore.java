package org.apache.mesos.hdfs.state;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Interface for storing and retrieving HDFS tracker resources.
 */
public interface HdfsStore {

  byte[] getFrameworkID() throws ExecutionException, InterruptedException;

  void setFrameworkId(byte[] frameworkId) throws ExecutionException, InterruptedException;

  <T extends Object> T get(String key) throws InterruptedException, ExecutionException,
    IOException, ClassNotFoundException;

  <T extends Object> void set(String key, T object) throws InterruptedException,
    ExecutionException, IOException;

}
