package org.apache.mesos.hdfs.executor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.stream.StreamUtil;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * The Task class for use within the executor.
 */
public class NodeHealthChecker {

  private final Log log = LogFactory.getLog(NodeHealthChecker.class);

  public NodeHealthChecker() {
  }

  public boolean runHealthCheckForTask(ExecutorDriver driver, Task task) {
    String taskIdStr = task.getTaskInfo().getTaskId().getValue();
    int healthCheckPort = getHealthCheckPort(taskIdStr);
    boolean taskHealthy = false;

    if (healthCheckPort != -1) {
      String healthCheckErrStr = "Error in node health check: ";
      String addressInUseStr = "Address already in use";
      Socket socket = null;
      try {
        // TODO (elingg) with better process supervision, check which process is
        // bound to the port.
        // Also, possibly do a http check for the name node UI as an additional
        // health check.
        String localhostAddress = InetAddress.getLocalHost().getHostAddress();
        socket = new Socket();
        socket.bind(new InetSocketAddress(localhostAddress, healthCheckPort));
      } catch (IOException e) {
        if (e.getMessage().contains(addressInUseStr)) {
          taskHealthy = true;
          log.info("Could not bind to port " + healthCheckPort + ", port is in use as expected.");
        } else {
          log.error(healthCheckErrStr, e);
        }
      } catch (SecurityException | IllegalArgumentException e) {
        log.error(healthCheckErrStr, e);
      }
      StreamUtil.closeQuietly(socket);
    }

    return taskHealthy;
  }

  private int getHealthCheckPort(String taskIdStr) {
    int healthCheckPort = -1;

    if (taskIdStr.contains(HDFSConstants.DATA_NODE_ID)) {
      healthCheckPort = HDFSConstants.DATA_NODE_PORT;
    } else if (taskIdStr.contains(HDFSConstants.JOURNAL_NODE_ID)) {
      healthCheckPort = HDFSConstants.JOURNAL_NODE_PORT;
    } else if (taskIdStr.contains(HDFSConstants.ZKFC_NODE_ID)) {
      healthCheckPort = HDFSConstants.ZKFC_NODE_PORT;
    } else if (taskIdStr.contains(HDFSConstants.NAME_NODE_ID)) {
      healthCheckPort = HDFSConstants.NAME_NODE_PORT;
    } else {
      log.error("Task unknown: " + taskIdStr);
    }

    return healthCheckPort;
  }

}
