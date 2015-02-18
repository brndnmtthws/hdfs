package org.apache.mesos.hdfs.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.*;

@Singleton
public class LiveState {
  private Set<Protos.TaskInfo> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.JOURNAL_NODES;
  private LinkedHashMap<Protos.TaskID, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();

  public void addStagingTask(Protos.TaskInfo taskInfo) {
    stagingTasks.add(taskInfo);
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    Set<Protos.TaskInfo> toRemove = new HashSet<>();
    for (Protos.TaskInfo taskInfo : stagingTasks ) {
      if (taskInfo.getTaskId().equals(taskID)) {
        toRemove.add(taskInfo);
      }
    }
    stagingTasks.removeAll(toRemove);
  }

  public void removeTask(Protos.TaskID taskId) {
    runningTasks.remove(taskId);
  }

  // TODO(rubbish): how do we get hostnames from the status, where do we look that up? the initial offer?
  // should we use a Map<TaskInfo,String> for the stagingTasks?
  // do we just keep a map of slave id to hostname for all the offers
  public void updateTaskForStatus(Protos.TaskStatus status) {
    runningTasks.put(status.getTaskId(), status);
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
    this.currentAcquisitionPhase = phase;
  }

  public int getJournalNodeSize() {
    return countOfRunningTasksWith(HDFSConstants.JOURNAL_NODE_ID);
  }

  public int getNameNodeSize() {
    return countOfRunningTasksWith(HDFSConstants.NAME_NODE_ID);
  }

  public Protos.TaskID getFirstNameNodeTaskId() {
    if (getNameNodeSize() >= 1) {
      return getNamenodeTaskIds().get(0);
    } else {
      return null;
    }
  }

  public Protos.TaskID getSecondNameNodeTaskId() {
    if (getNameNodeSize() == 2) {
      return getNamenodeTaskIds().get(1);
    } else {
      return null;
    }
  }


  public Protos.SlaveID getFirstNameNodeSlaveId() {
    if (getNameNodeSize() >= 1) {
      return getNamenodeSlaveIds().get(0);
    } else {
      return null;
    }
  }

  public Protos.SlaveID getSecondNameNodeSlaveId() {
    if (getNameNodeSize() == 2) {
      return getNamenodeSlaveIds().get(1);
    } else {
      return null;
    }
  }

  private ArrayList<Protos.TaskID> getNamenodeTaskIds() {
    ArrayList<Protos.TaskID> namenodes = new ArrayList();
    for (Protos.TaskStatus taskStatus : runningTasks.values()) {
      if (taskStatus.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
        namenodes.add(taskStatus.getTaskId());
      }
    }
    return namenodes;
  }

  private ArrayList<Protos.SlaveID> getNamenodeSlaveIds() {
    ArrayList<Protos.SlaveID> namenodes = new ArrayList();
    for (Protos.TaskStatus taskStatus : runningTasks.values()) {
      if (taskStatus.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_ID)) {
        namenodes.add(taskStatus.getSlaveId());
      }
    }
    return namenodes;
  }

  private int countOfRunningTasksWith(final String nodeId) {
    return Sets.filter(runningTasks.keySet(), new Predicate<Protos.TaskID>() {
      @Override
      public boolean apply(Protos.TaskID taskID) {
        return taskID.getValue().contains(nodeId);
      }
    }).size();
  }
}
