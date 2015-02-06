package org.apache.mesos.hdfs;

import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.state.LiveState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestLiveState {

  private LiveState liveState;

  @Test
  public void getsJournalNodeSize() {
    liveState.updateTaskForStatus(createTaskStatus("journalnode.2"));
    liveState.updateTaskForStatus(createTaskStatus("journalnode.1"));
    liveState.updateTaskForStatus(createTaskStatus("datanode1"));

    assertEquals(2, liveState.getJournalNodeSize());
  }

  @Test
  public void getsNameNodeSize() {
    liveState.updateTaskForStatus(createTaskStatus("journalnode.1"));
    liveState.updateTaskForStatus(createTaskStatus("namenode.2"));
    liveState.updateTaskForStatus(createTaskStatus("namenode.1"));
    liveState.updateTaskForStatus(createTaskStatus("datanode1"));

    assertEquals(2, liveState.getNameNodeSize());
  }

  @Before
  public void setup() {
    liveState = new LiveState();
  }

  private Protos.TaskStatus createTaskStatus(String taskId) {
    return Protos.TaskStatus.newBuilder()
        .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave1"))
        .setTaskId(Protos.TaskID.newBuilder().setValue(taskId))
        .setState(Protos.TaskState.TASK_RUNNING)
        .build();
  }

  //  @Test
//  public void testNameNodeTask() {
//    LiveState clusterState = new LiveState();
//
//    TaskID taskId = TaskID.newBuilder()
//        .setValue(".namenode.namenode.123")
//        .build();
//
//    SlaveID slaveId = SlaveID.newBuilder()
//        .setValue("worker10.19.15.1")
//        .build();
//
//    // Add task
//    clusterState.addTask(taskId, "10.19.15.1", "worker10.19.15.1");
//
//    TaskStatus taskStatus = TaskStatus.newBuilder()
//        .setTaskId(taskId)
//        .setSlaveId(slaveId)
//        .setState(TaskState.TASK_RUNNING)
//        .build();
//
//    // Update task
//    clusterState.updateTask(taskStatus);
//
//    assertTrue(clusterState.getNameNodes().contains(taskId));
//    assertFalse(clusterState.notInDfsHosts(slaveId.getValue()));
//    assertTrue(clusterState.getNameNodeHosts().contains("10.19.15.1"));
//  }
//
//  @Test
//  public void testJournalNodeTask() {
//    LiveState clusterState = new LiveState();
//
//    TaskID taskId = TaskID.newBuilder()
//        .setValue(".namenode.journalnode.123")
//        .build();
//
//    SlaveID slaveId = SlaveID.newBuilder()
//        .setValue("worker10.80.16.2")
//        .build();
//
//    // Add task
//    clusterState.addTask(taskId, "10.80.16.2", "worker10.80.16.2");
//
//    TaskStatus taskStatus = TaskStatus.newBuilder()
//        .setTaskId(taskId)
//        .setSlaveId(slaveId)
//        .setState(TaskState.TASK_RUNNING)
//        .build();
//
//    // Update task
//    clusterState.updateTask(taskStatus);
//
//    assertTrue(clusterState.getJournalNodes().contains(taskId));
//    assertFalse(clusterState.notInDfsHosts(slaveId.getValue()));
//    assertTrue(clusterState.getJournalNodeHosts().contains("10.80.16.2"));
//
//    // Remove task
//    clusterState.removeTask(taskStatus);
//
//    assertFalse(clusterState.getJournalNodes().contains(taskId));
//    assertTrue(clusterState.notInDfsHosts(slaveId.getValue()));
//    assertFalse(clusterState.getJournalNodeHosts().contains("10.80.16.2"));
//  }
}
