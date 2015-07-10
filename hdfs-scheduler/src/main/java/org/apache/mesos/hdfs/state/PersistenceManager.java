package org.apache.mesos.hdfs.state;

import org.apache.mesos.Protos;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Entry point for persistence for the HDFS scheduler.
 */
public interface PersistenceManager {

  void setFrameworkId(Protos.FrameworkID id);

  Protos.FrameworkID getFrameworkId();

  void removeTaskId(String taskId);

  Set<String> getAllTaskIds();

  void addHdfsNode(Protos.TaskID taskId, String hostname, String taskType, String taskName);

  Map<String, String> getNameNodeTaskNames();

  Map<String, String> getJournalNodeTaskNames();

  List<String> getDeadJournalNodes();

  List<String> getDeadNameNodes();

  List<String> getDeadDataNodes();

  Map<String, String> getJournalNodes();

  Map<String, String> getNameNodes();

  Map<String, String> getDataNodes();

  boolean journalNodeRunningOnSlave(String hostname);

  boolean dataNodeRunningOnSlave(String hostname);

  boolean nameNodeRunningOnSlave(String hostname);

}
