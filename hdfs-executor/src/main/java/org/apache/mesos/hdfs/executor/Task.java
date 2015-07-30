package org.apache.mesos.hdfs.executor;

import org.apache.mesos.Protos;

/**
 * The Task class for use within the executor.
 */
public class Task {

  private Protos.TaskInfo taskInfo;
  private String cmd;
  private Process process;

  public Task(Protos.TaskInfo taskInfo) {
    this.taskInfo = taskInfo;
    this.cmd = taskInfo.getData().toStringUtf8();
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
}
