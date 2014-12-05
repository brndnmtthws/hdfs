package org.apache.mesos.hdfs;

import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.state.ClusterState;
import org.apache.mesos.hdfs.Scheduler.DfsTask;
import com.google.inject.Guice;
import com.google.inject.Injector;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.Test;

public class TestClusterState {

  @Test
  public void testNameNodeTask() {
    ClusterState clusterState = ClusterState.getInstance();

    TaskID taskId = TaskID.newBuilder()
        .setValue("namenode")
        .build();

    SlaveID slaveId = SlaveID.newBuilder()
        .setValue("worker10.19.15.1")
        .build();

    TaskInfo task = TaskInfo.newBuilder()
        .setName("namenode")
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .build();

    // Add task
    clusterState.addTask(taskId,
        new DfsTask("namenode", slaveId.getValue(), "10.19.15.1"));

    TaskStatus taskStatus = TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setState(TaskState.TASK_RUNNING)
        .build();

    // Update task
    clusterState.updateTask(taskStatus);

    assertTrue(clusterState.getNamenodes().contains(taskId));
    assertFalse(clusterState.notInDfsHosts(slaveId.getValue()));
    assertTrue(clusterState.getNamenodeHosts().contains("10.19.15.1"));
  }

  @Test
  public void testJournalNodeTask() {
    ClusterState clusterState = ClusterState.getInstance();

    TaskID taskId = TaskID.newBuilder()
        .setValue("journalnode")
        .build();

    SlaveID slaveId = SlaveID.newBuilder()
        .setValue("worker10.80.16.2")
        .build();

    TaskInfo task = TaskInfo.newBuilder()
        .setName("journalnode")
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .build();

    // Add task
    clusterState.addTask(taskId,
        new DfsTask("journalnode", slaveId.getValue(), "10.80.16.2"));

    TaskStatus taskStatus = TaskStatus.newBuilder()
        .setTaskId(taskId)
        .setSlaveId(slaveId)
        .setState(TaskState.TASK_RUNNING)
        .build();

    // Update task
    clusterState.updateTask(taskStatus);

    assertTrue(clusterState.getJournalnodes().contains(taskId));
    assertFalse(clusterState.notInDfsHosts(slaveId.getValue()));
    assertTrue(clusterState.getJournalnodeHosts().contains("10.80.16.2"));

    // Remove task
    clusterState.removeTask(taskStatus);

    assertFalse(clusterState.getJournalnodes().contains(taskId));
    assertTrue(clusterState.notInDfsHosts(slaveId.getValue()));
    assertFalse(clusterState.getJournalnodeHosts().contains("10.80.16.2"));
  }
}
