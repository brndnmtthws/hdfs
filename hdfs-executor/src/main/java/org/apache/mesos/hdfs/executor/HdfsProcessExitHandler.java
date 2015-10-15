package org.apache.mesos.hdfs.executor;

import org.apache.mesos.process.FailureUtils;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.process.ProcessFailureHandler;

/**
 * When a process fails this handler will exit the JVM.
 */
public class HdfsProcessExitHandler implements ProcessFailureHandler {
  public void handle() {
    FailureUtils.exit("Task Process Failed", HDFSConstants.PROC_EXIT_CODE);
  }
}
