package org.apache.mesos.hdfs.state;

import com.google.inject.Singleton;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.hdfs.util.HDFSConstants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Singleton
public class LiveState {
  private static final Log log = LogFactory.getLog(LiveState.class);
  private final Map<Protos.TaskID, String> taskHostMap;
  private final Set<Protos.TaskID> journalNodes;
  private final Set<Protos.TaskID> nameNodes;
  private final Map<Protos.TaskID, String> taskSlaveMap;
  private final Set<String> journalNodeHosts;
  private final Set<String> nameNodeHosts;

  public LiveState() {
    taskHostMap = new HashMap<>();
    taskSlaveMap = new HashMap<>();
    journalNodes = new HashSet<>();
    nameNodes = new HashSet<>();
    journalNodeHosts = new HashSet<>();
    nameNodeHosts = new HashSet<>();
  }

  public Map<Protos.TaskID, String> getTaskHostMap() {
    return taskHostMap;
  }

  public Map<Protos.TaskID, String> getTaskSlaveMap() {
    return taskSlaveMap;
  }

  public Set<Protos.TaskID> getJournalNodes() {
    return journalNodes;
  }

  public Set<Protos.TaskID> getNameNodes() {
    return nameNodes;
  }

  public Set<String> getJournalNodeHosts() {
    return journalNodeHosts;
  }

  public Set<String> getNameNodeHosts() {
    return nameNodeHosts;
  }

  public boolean notInDfsHosts(String slaveId) {
    return !taskSlaveMap.values().contains(slaveId);
  }

  public void addTask(Protos.TaskID taskId, String hostname, String slaveId) {
    taskHostMap.put(taskId, hostname);
    taskSlaveMap.put(taskId, slaveId);
    if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodeHosts.add(hostname);
    } else if (taskId.getValue().contains(HDFSConstants.JOURNAL_NODE_ID)) {
      journalNodeHosts.add(hostname);
    }
  }

  public void updateTask(Protos.TaskStatus taskStatus) {
    if (taskStatus.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      nameNodes.add(taskStatus.getTaskId());
    } else if (taskStatus.getTaskId().getValue()
        .contains(HDFSConstants.JOURNAL_NODE_ID)) {
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
