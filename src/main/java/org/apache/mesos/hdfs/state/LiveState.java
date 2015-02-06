package org.apache.mesos.hdfs.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.mesos.Protos;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

@Singleton
public class LiveState {
  private Set<Protos.TaskInfo> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.JOURNAL_NODES;
  private Map<Protos.TaskID, Protos.TaskStatus> lastKnownStatuses = new TreeMap<>();

  public void addStagingTask(Protos.TaskInfo taskInfo) {
    stagingTasks.add(taskInfo);
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    Sets.filter(stagingTasks, new Predicate<Protos.TaskInfo>() {
      @Override
      public boolean apply(Protos.TaskInfo taskInfo) {
        return taskInfo.getTaskId().equals(taskID);
      }
    });
  }

  public void removeTask(Protos.TaskID taskId) {
    lastKnownStatuses.remove(taskId);
  }

  // TODO(rubbish): how do we get hostnames from the status, where do we look that up? the initial offer?
  // should we use a Map<TaskInfo,String> for the stagingTasks?
  // do we just keep a map of slave id to hostname for all the offers
  public void updateTaskForStatus(Protos.TaskStatus status) {
    lastKnownStatuses.put(status.getTaskId(), status);
  }

  public AcquisitionPhase getCurrentAcquisitionPhase() {
    return currentAcquisitionPhase;
  }

  public void transitionTo(AcquisitionPhase phase) {
    this.currentAcquisitionPhase = phase;
  }

  public int getJournalNodeSize() {
    return 0;
  }

  public int getNameNodeSize() {
    return 0;
  }

  public Protos.TaskID getFirstNameNodeTaskId() {
    return null;
  }

  public Protos.TaskID getSecondNameNodeTaskId() {
    return null;
  }

  public Protos.SlaveID getFirstNameNodeSlaveId() {
    return null;
  }

  public Protos.SlaveID getSecondNameNodeSlaveId() {
    return null;
  }
}
