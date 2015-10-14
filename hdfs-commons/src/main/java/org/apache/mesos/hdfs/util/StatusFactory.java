package org.apache.mesos.hdfs.util;

import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

/**
 * Class to generate TaskStatus messages.
 */
public class StatusFactory {
  public static TaskStatus createNameNodeStatus(TaskID taskId, boolean initialized) {
    String initStatus = getNameNodeInitStatus(initialized);

    return TaskStatus.newBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_RUNNING)
      .setLabels(Labels.newBuilder()
        .addLabels(Label.newBuilder()
          .setKey(HDFSConstants.NN_STATUS_KEY)
          .setValue(initStatus)))
      .build();
  }

  public static TaskStatus createRunningStatus(TaskID taskId) {
    return TaskStatus.newBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_RUNNING)
      .build();
  }

  public static TaskStatus createKilledStatus(TaskID taskId) {
    return TaskStatus.newBuilder()
      .setTaskId(taskId)
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
