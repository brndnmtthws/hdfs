package org.apache.mesos.hdfs.executor;

import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.util.HDFSConstants;

/**
 * The Task class for use within the executor.
 */
public class Task {

  private Protos.TaskInfo taskInfo;
  private String cmd;
  private Process process;
  private String type;

  public Task(Protos.TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
    this.cmd = taskInfo.getData().toStringUtf8();
    discoveryType(taskInfo.getTaskId().getValue());
  }

  private void discoveryType(String taskId) {
    type = "";
    if (taskId.contains("task." + HDFSConstants.JOURNAL_NODE_ID)) {
      type = HDFSConstants.JOURNAL_NODE_ID;
    } else if (taskId.contains("task." + HDFSConstants.NAME_NODE_ID)) {
      type = HDFSConstants.NAME_NODE_ID;
    } else if (taskId.contains("task." + HDFSConstants.ZKFC_NODE_ID)) {
      type = HDFSConstants.ZKFC_NODE_ID;
    } else if (taskId.contains("task." + HDFSConstants.DATA_NODE_ID)) {
      type = HDFSConstants.DATA_NODE_ID;
    }
  }

  public String getCmd() {
    return cmd;
  }

  public void setCmd(String cmd) {
    this.cmd = cmd;
  }

  public Process getProcess() {
    return process;
  }

  public void setProcess(Process process) {
    this.process = process;
  }

  public Protos.TaskInfo getTaskInfo() {
    return taskInfo;
  }

  public void setTaskInfo(Protos.TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "Task{" +
      "cmd='" + cmd + '\'' +
      ", taskInfo=" + taskInfo +
      ", type='" + type + '\'' +
      '}';
  }
}
