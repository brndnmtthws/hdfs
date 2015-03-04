package org.apache.mesos.hdfs.state;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import com.google.inject.Singleton;
import org.apache.commons.lang.time.DateUtils;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;

@Singleton
public class LiveState {
  private Set<Protos.TaskID> stagingTasks = new HashSet<>();
  private AcquisitionPhase currentAcquisitionPhase = AcquisitionPhase.RECONCILING_TASKS;
  // TODO (nicgrayson) Might need to split this out to jns, nns, and dns if dns too big
  private LinkedHashMap<Protos.TaskID, Protos.TaskStatus> runningTasks = new LinkedHashMap<>();
  private Timestamp ReconciliationTimestamp;
  private Protos.TaskStatus nameNode1TaskStatus = null;
  private Protos.TaskStatus nameNode2TaskStatus = null;
  private SchedulerConf schedulerConf = null;
    
  public LiveState(SchedulerConf conf) {
    schedulerConf = conf;
  }

  public boolean reconciliationComplete() {
    return ReconciliationTimestamp.before(new Date());
  }

  public boolean isNameNode1Initialized() {
    return nameNode1TaskStatus != null;
  }

  public boolean isNameNode2Initialized() {
    return nameNode2TaskStatus != null;
  }

  public void updateReconciliationTimestamp() {
    Date date = DateUtils.addSeconds(new Date(), schedulerConf.getReconciliationTimeout());
    ReconciliationTimestamp = new Timestamp(date.getTime());
  }

  public void addStagingTask(Protos.TaskID taskId) {
    stagingTasks.add(taskId);
  }

  public int getStagingTasksSize() {
    return stagingTasks.size();
  }

  public void removeStagingTask(final Protos.TaskID taskID) {
    stagingTasks.remove(taskID);
  }

  public LinkedHashMap<Protos.TaskID, Protos.TaskStatus> getRunningTasks() {
    return runningTasks;
  }

  public void removeRunningTask(Protos.TaskID taskId) {
    if (isNameNode1Initialized() && nameNode1TaskStatus.getTaskId().equals(taskId)) {
      nameNode1TaskStatus = null;
    } else if (isNameNode2Initialized()
        && nameNode2TaskStatus.getTaskId().equals(taskId)) {
      nameNode2TaskStatus = null;
    }
    runningTasks.remove(taskId);
  }

  public void updateTaskForStatus(Protos.TaskStatus status) {
    if (status.getMessage().equals(HDFSConstants.NAME_NODE_INIT_MESSAGE)) {
      nameNode1TaskStatus = status;
    } else if (status.getMessage().equals(HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE)) {
      nameNode2TaskStatus = status;
    }
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
    return countOfRunningTasksWith(HDFSConstants.NAME_NODE_TASKID);
  }

  public Protos.TaskID getFirstNameNodeTaskId() {
    if (nameNode1TaskStatus == null) {
      return null;
    }
    return nameNode1TaskStatus.getTaskId();
  }

  public Protos.TaskID getSecondNameNodeTaskId() {
    if (nameNode2TaskStatus == null) {
      return null;
    }
    return nameNode2TaskStatus.getTaskId();
  }

  public Protos.SlaveID getFirstNameNodeSlaveId() {
    if (nameNode1TaskStatus == null) {
      return null;
    }
    return nameNode1TaskStatus.getSlaveId();
  }

  public Protos.SlaveID getSecondNameNodeSlaveId() {
    if (nameNode2TaskStatus == null) {
      return null;
    }
    return nameNode2TaskStatus.getSlaveId();
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
