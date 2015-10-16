package org.apache.mesos.hdfs.util;

import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.protobuf.TaskStatusBuilder;

/**
 * Class to generate TaskStatus messages.
 */
public class TaskStatusFactory {
  public static TaskStatus createNameNodeStatus(TaskID taskId, boolean initialized) {
    String initStatus = getNameNodeInitStatus(initialized);

    return new TaskStatusBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_RUNNING)
      .addLabel(HDFSConstants.NN_STATUS_KEY, initStatus)
      .build();
  }

  public static TaskStatus createRunningStatus(TaskID taskId) {
    return new TaskStatusBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_RUNNING)
      .build();
  }

  public static TaskStatus createKilledStatus(TaskID taskId) {
    return new TaskStatusBuilder()
      .setTaskId(taskId.getValue())
      .setState(TaskState.TASK_KILLED)
      .build();
  }

  private static String getNameNodeInitStatus(boolean initialized) {
    if (initialized) {
      return HDFSConstants.NN_STATUS_INIT_VAL;
    } else {
      return HDFSConstants.NN_STATUS_UNINIT_VAL;
    }
  }
}
