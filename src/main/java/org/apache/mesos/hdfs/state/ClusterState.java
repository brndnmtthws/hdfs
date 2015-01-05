package org.apache.mesos.hdfs.state;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.Scheduler;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ClusterState {
  private static final Log log = LogFactory.getLog(ClusterState.class);
  private State state;
  private final Map<Protos.TaskID, String> taskHostMap;
  private final Set<Protos.TaskID> journalNodes;
  private final Set<Protos.TaskID> nameNodes;
  private final Map<Protos.TaskID, String> taskSlaveMap;
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
    taskHostMap = new HashMap<>();
    taskSlaveMap = new HashMap<>();
    journalNodes = new HashSet<>();
    nameNodes = new HashSet<>();
    journalNodeHosts = new HashSet<>();
    nameNodeHosts = new HashSet<>();
  }

  public void init(State state) {
    this.state = state;
  }

  public State getState() {
    return state;
  }

  public final Map<Protos.TaskID, String> getTaskHostMap() {
    return taskHostMap;
  }

  public final Map<Protos.TaskID, String> getTaskSlaveMap() {
    return taskSlaveMap;
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
    return !taskSlaveMap.values().contains(host);
  }

  public void addTask(Protos.TaskID taskId, String hostname, String slaveId) {
    taskHostMap.put(taskId, hostname);
    taskSlaveMap.put(taskId, slaveId);
    if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodeHosts.add(hostname);
    } else if (taskId.getValue().contains(HDFSConstants.JOURNAL_NODE_TASKID)) {
      journalNodeHosts.add(hostname);
    }
  }

  public void updateTask(Protos.TaskStatus taskStatus) {
    if (taskStatus.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodes.add(taskStatus.getTaskId());
    } else if (taskStatus.getTaskId().getValue()
        .contains(HDFSConstants.JOURNAL_NODE_TASKID)) {
      journalNodes.add(taskStatus.getTaskId());
    }
  }

  public void removeTask(Protos.TaskStatus taskStatus) {
    Protos.TaskID taskId = taskStatus.getTaskId();
    journalNodeHosts.remove(taskHostMap.get(taskId));
    nameNodeHosts.remove(taskHostMap.get(taskId));
    taskHostMap.remove(taskId);
    nameNodes.remove(taskId);
    journalNodes.remove(taskId);
    taskSlaveMap.remove(taskId);
  }
}
