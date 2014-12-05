package org.apache.mesos.hdfs.state;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.Scheduler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClusterState {
  private static final Log log = LogFactory.getLog(ClusterState.class);
  private State state;
  private final Map<Protos.TaskID, Scheduler.DfsTask> tasks;
  private final Set<Protos.TaskID> journalNodes;
  private final Set<Protos.TaskID> nameNodes;
  private final Set<String> dfsHosts;
  private final Set<String> journalNodeHosts;
  private final Set<String> nameNodeHosts;

  private static ClusterState instance = null;

  public static ClusterState getInstance() {
    if (instance == null) {
      instance = new ClusterState();
    }
    return instance;
  }

  private ClusterState() {
    tasks = new HashMap<>();
    journalNodes = new HashSet<>();
    nameNodes = new HashSet<>();
    dfsHosts = new HashSet<>();
    journalNodeHosts = new HashSet<>();
    nameNodeHosts = new HashSet<>();
  }

  public void init(State state) {
    this.state = state;
  }

  public void clear() {
    tasks.clear();
    journalNodes.clear();
    nameNodes.clear();
    dfsHosts.clear();
    journalNodeHosts.clear();
    nameNodeHosts.clear();
  }

  public State getState() {
    return state;
  }

  public final Map<Protos.TaskID, Scheduler.DfsTask> getTasks() {
    return tasks;
  }

  public final Scheduler.DfsTask getDfsTask(Protos.TaskID taskID) {
    return tasks.get(taskID);
  }

  public final Set<Protos.TaskID> getJournalNodes() {
    return journalNodes;
  }

  public final Set<Protos.TaskID> getNameNodes() {
    return nameNodes;
  }

  public final Set<String> getJournalNodeHosts() {
    return journalNodeHosts;
  }

  public final Set<String> getNameNodeHosts() {
    return nameNodeHosts;
  }

  public boolean notInDfsHosts(String host) {
    return !dfsHosts.contains(host);
  }

  public void addTask(Protos.TaskID taskId, Scheduler.DfsTask dfsTask) {
    tasks.put(taskId, dfsTask);
    Scheduler.DfsTask.Type type = dfsTask.type;
    switch (type) {
      case NN :
        nameNodeHosts.add(dfsTask.hostname);
        break;
      case JN :
        journalNodeHosts.add(dfsTask.hostname);
        break;
      default :
        break;
    }
  }

  public void updateTask(Protos.TaskStatus taskStatus) {
    if (tasks.containsKey(taskStatus.getTaskId())) {
      tasks.get(taskStatus.getTaskId()).taskStatus = taskStatus;
      Scheduler.DfsTask.Type type = tasks.get(taskStatus.getTaskId()).type;
      dfsHosts.add(taskStatus.getSlaveId().getValue());
      switch (type) {
        case DN :
        case ZKFC :
          break;
        case NN :
          nameNodes.add(taskStatus.getTaskId());
          break;
        case JN :
          journalNodes.add(taskStatus.getTaskId());
          break;
        default :
          Scheduler.log.error("Unknown task type: " + type);
          break;
      }
    } else {
      log.error("Asked to update unknown task, taskId=" + taskStatus.getTaskId().getValue());
    }
  }

  public void removeTask(Protos.TaskStatus taskStatus) {
    Protos.TaskID taskId = taskStatus.getTaskId();
    if (tasks.containsKey(taskId) && tasks.get(taskId).hostname != null) {
      journalNodeHosts.remove(tasks.get(taskId).hostname);
      nameNodeHosts.remove(tasks.get(taskId).hostname);
    }
    tasks.remove(taskId);
    nameNodes.remove(taskId);
    journalNodes.remove(taskId);
    dfsHosts.remove(taskStatus.getSlaveId().getValue());
  }
}
