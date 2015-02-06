package org.apache.mesos.hdfs;

public class TestLiveState {

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
