package org.apache.mesos.hdfs.state;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Manages the persists needs of the scheduler.  It handles frameworkID and the list of tasks on each host.
 */
@Singleton
public class PersistentState {
  private final Log log = LogFactory.getLog(PersistentState.class);

  private static final String FRAMEWORK_ID_KEY = "frameworkId";
  private static final String NAMENODES_KEY = "nameNodes";
  private static final String JOURNALNODES_KEY = "journalNodes";
  private static final String DATANODES_KEY = "dataNodes";
  private static final String NAMENODE_TASKNAMES_KEY = "nameNodeTaskNames";
  private static final String JOURNALNODE_TASKNAMES_KEY = "journalNodeTaskNames";

  private State zkState;
  private HdfsFrameworkConfig hdfsFrameworkConfig;
  // TODO (elingg) we need to also track ZKFC's state

  private Timestamp deadJournalNodeTimeStamp = null;
  private Timestamp deadNameNodeTimeStamp = null;
  private Timestamp deadDataNodeTimeStamp = null;

  @Inject
  public PersistentState(HdfsFrameworkConfig hdfsFrameworkConfig) {
    MesosNativeLibrary.load(hdfsFrameworkConfig.getNativeLibrary());
    this.zkState = new ZooKeeperState(hdfsFrameworkConfig.getStateZkServers(),
      hdfsFrameworkConfig.getStateZkTimeout(),
      TimeUnit.MILLISECONDS,
      "/hdfs-mesos/" + hdfsFrameworkConfig.getFrameworkName());
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
    resetDeadNodeTimeStamps();
  }

  public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException, InvalidProtocolBufferException {
    byte[] existingFrameworkId = zkState.fetch(FRAMEWORK_ID_KEY).get().value();
    if (existingFrameworkId.length > 0) {
      return FrameworkID.parseFrom(existingFrameworkId);
    } else {
      return null;
    }
  }

  public void setFrameworkId(FrameworkID frameworkId) throws InterruptedException,
    ExecutionException {
    Variable value = zkState.fetch(FRAMEWORK_ID_KEY).get();
    value = value.mutate(frameworkId.toByteArray());
    zkState.store(value).get();
  }

  private void resetDeadNodeTimeStamps() {
    Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());

    if (getDeadJournalNodes().size() > 0) {
      deadJournalNodeTimeStamp = new Timestamp(date.getTime());
    }

    if (getDeadNameNodes().size() > 0) {
      deadNameNodeTimeStamp = new Timestamp(date.getTime());
    }

    if (getDeadDataNodes().size() > 0) {
      deadDataNodeTimeStamp = new Timestamp(date.getTime());
    }
  }

  private void removeDeadJournalNodes() {
    deadJournalNodeTimeStamp = null;
    Map<String, String> journalNodes = getJournalNodes();
    List<String> deadJournalHosts = getDeadJournalNodes();
    for (String deadJournalHost : deadJournalHosts) {
      journalNodes.remove(deadJournalHost);
      log.info("Removing JN Host: " + deadJournalHost);
    }
    setJournalNodes(journalNodes);
  }

  private void removeDeadNameNodes() {
    deadNameNodeTimeStamp = null;
    Map<String, String> nameNodes = getNameNodes();
    List<String> deadNameHosts = getDeadNameNodes();
    for (String deadNameHost : deadNameHosts) {
      nameNodes.remove(deadNameHost);
      log.info("Removing NN Host: " + deadNameHost);
    }
    setNameNodes(nameNodes);
  }

  private void removeDeadDataNodes() {
    deadDataNodeTimeStamp = null;
    Map<String, String> dataNodes = getDataNodes();
    List<String> deadDataHosts = getDeadDataNodes();
    for (String deadDataHost : deadDataHosts) {
      dataNodes.remove(deadDataHost);
      log.info("Removing DN Host: " + deadDataHost);
    }
    setDataNodes(dataNodes);
  }

  public List<String> getDeadJournalNodes() {
    List<String> deadJournalHosts = new ArrayList<>();

    if (deadJournalNodeTimeStamp != null && deadJournalNodeTimeStamp.before(new Date())) {
      removeDeadJournalNodes();
    } else {
      Map<String, String> journalNodes = getJournalNodes();
      final Set<Map.Entry<String, String>> journalEntries = journalNodes.entrySet();
      for (Map.Entry<String, String> journalNode : journalEntries) {
        if (journalNode.getValue() == null) {
          deadJournalHosts.add(journalNode.getKey());
        }
      }
    }
    return deadJournalHosts;
  }

  public List<String> getDeadNameNodes() {
    List<String> deadNameHosts = new ArrayList<>();

    if (deadNameNodeTimeStamp != null && deadNameNodeTimeStamp.before(new Date())) {
      removeDeadNameNodes();
    } else {
      Map<String, String> nameNodes = getNameNodes();
      final Set<Map.Entry<String, String>> nameNodeEntries = nameNodes.entrySet();
      for (Map.Entry<String, String> nameNode : nameNodeEntries) {
        if (nameNode.getValue() == null) {
          deadNameHosts.add(nameNode.getKey());
        }
      }
    }
    return deadNameHosts;
  }

  public List<String> getDeadDataNodes() {
    List<String> deadDataHosts = new ArrayList<>();

    if (deadDataNodeTimeStamp != null && deadDataNodeTimeStamp.before(new Date())) {
      removeDeadDataNodes();
    } else {
      Map<String, String> dataNodes = getDataNodes();
      final Set<Map.Entry<String, String>> dataNodeEntries = dataNodes.entrySet();
      for (Map.Entry<String, String> dataNode : dataNodeEntries) {
        if (dataNode.getValue() == null) {
          deadDataHosts.add(dataNode.getKey());
        }
      }
    }
    return deadDataHosts;
  }

  // TODO (nicgrayson) add tests with in-memory state implementation for zookeeper
  public Map<String, String> getJournalNodes() {
    return getHashMap(JOURNALNODES_KEY);
  }

  public Map<String, String> getNameNodes() {
    return getHashMap(NAMENODES_KEY);
  }

  public Map<String, String> getJournalNodeTaskNames() {
    return getHashMap(JOURNALNODE_TASKNAMES_KEY);
  }

  public Map<String, String> getNameNodeTaskNames() {
    return getHashMap(NAMENODE_TASKNAMES_KEY);
  }

  public Map<String, String> getDataNodes() {
    return getHashMap(DATANODES_KEY);
  }

  public Set<String> getAllTaskIds() {
    Set<String> allTaskIds = new HashSet<String>();
    Collection<String> journalNodes = getJournalNodes().values();
    Collection<String> nameNodes = getNameNodes().values();
    Collection<String> dataNodes = getDataNodes().values();
    allTaskIds.addAll(journalNodes);
    allTaskIds.addAll(nameNodes);
    allTaskIds.addAll(dataNodes);
    return allTaskIds;
  }

  public void addHdfsNode(Protos.TaskID taskId, String hostname, String taskType, String taskName) {
    switch (taskType) {
      case HDFSConstants.NAME_NODE_ID:
        Map<String, String> nameNodes = getNameNodes();
        nameNodes.put(hostname, taskId.getValue());
        setNameNodes(nameNodes);
        Map<String, String> nameNodeTaskNames = getNameNodeTaskNames();
        nameNodeTaskNames.put(taskId.getValue(), taskName);
        setNameNodeTaskNames(nameNodeTaskNames);
        break;
      case HDFSConstants.JOURNAL_NODE_ID:
        Map<String, String> journalNodes = getJournalNodes();
        journalNodes.put(hostname, taskId.getValue());
        setJournalNodes(journalNodes);
        Map<String, String> journalNodeTaskNames = getJournalNodeTaskNames();
        journalNodeTaskNames.put(taskId.getValue(), taskName);
        setJournalNodeTaskNames(journalNodeTaskNames);
        break;
      case HDFSConstants.DATA_NODE_ID:
        Map<String, String> dataNodes = getDataNodes();
        dataNodes.put(hostname, taskId.getValue());
        setDataNodes(dataNodes);
        break;
      case HDFSConstants.ZKFC_NODE_ID:
        break;
      default:
        log.error("Task name unknown");
    }
  }

  // TODO (elingg) optimize this method/ Possibly index by task id instead of hostname/
  // Possibly call removeTask(slaveId, taskId) to avoid iterating through all maps
  public void removeTaskId(String taskId) {

    Map<String, String> journalNodes = getJournalNodes();
    if (journalNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : journalNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          journalNodes.put(entry.getKey(), null);
          setJournalNodes(journalNodes);
          Map<String, String> journalNodeTaskNames = getJournalNodeTaskNames();
          journalNodeTaskNames.remove(taskId);
          setJournalNodeTaskNames(journalNodeTaskNames);
          Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
          deadJournalNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }

    Map<String, String> nameNodes = getNameNodes();
    if (nameNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : nameNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          nameNodes.put(entry.getKey(), null);
          setNameNodes(nameNodes);
          Map<String, String> nameNodeTaskNames = getNameNodeTaskNames();
          nameNodeTaskNames.remove(taskId);
          setNameNodeTaskNames(nameNodeTaskNames);
          Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
          deadNameNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }

    Map<String, String> dataNodes = getDataNodes();
    if (dataNodes.values().contains(taskId)) {
      for (Map.Entry<String, String> entry : dataNodes.entrySet()) {
        if (entry.getValue() != null && entry.getValue().equals(taskId)) {
          dataNodes.put(entry.getKey(), null);
          setDataNodes(dataNodes);
          Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
          deadDataNodeTimeStamp = new Timestamp(date.getTime());
          return;
        }
      }
    }
  }

  public boolean journalNodeRunningOnSlave(String hostname) {
    return getJournalNodes().containsKey(hostname);
  }

  public boolean nameNodeRunningOnSlave(String hostname) {
    return getNameNodes().containsKey(hostname);
  }

  public boolean dataNodeRunningOnSlave(String hostname) {
    return getDataNodes().containsKey(hostname);
  }

  private void setNameNodes(Map<String, String> nameNodes) {
    try {
      set(NAMENODES_KEY, nameNodes);
    } catch (Exception e) {
      log.error("Error while setting namenodes in persistent state", e);
    }
  }

  private void setJournalNodes(Map<String, String> journalNodes) {
    try {
      set(JOURNALNODES_KEY, journalNodes);
    } catch (Exception e) {
      log.error("Error while setting journalnodes in persistent state", e);
    }
  }

  private void setNameNodeTaskNames(Map<String, String> nameNodeTaskNames) {
    try {
      set(NAMENODE_TASKNAMES_KEY, nameNodeTaskNames);
    } catch (Exception e) {
      log.error("Error while setting namenodes in persistent state", e);
    }
  }

  private void setJournalNodeTaskNames(Map<String, String> journalNodeTaskNames) {
    try {
      set(JOURNALNODE_TASKNAMES_KEY, journalNodeTaskNames);
    } catch (Exception e) {
      log.error("Error while setting journalnodes in persistent state", e);
    }
  }

  private void setDataNodes(Map<String, String> dataNodes) {
    try {
      set(DATANODES_KEY, dataNodes);
    } catch (Exception e) {
      log.error("Error while setting datanodes in persistent state", e);
    }
  }

  private Map<String, String> getHashMap(String key) {
    try {
      HashMap<String, String> nodesMap = get(key);
      if (nodesMap == null) {
        return new HashMap<>();
      }
      return nodesMap;
    } catch (Exception e) {
      log.error(String.format("Error while getting %s in persistent state", key), e);
      return new HashMap<>();
    }
  }

  /**
   * Get serializable object from store.
   *
   * @return serialized object or null if none
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   * @throws ClassNotFoundException
   */
  @SuppressWarnings("unchecked")
  private <T extends Object> T get(String key) throws InterruptedException, ExecutionException,
    IOException, ClassNotFoundException {
    byte[] existingNodes = zkState.fetch(key).get().value();
    if (existingNodes.length > 0) {
      ByteArrayInputStream bis = new ByteArrayInputStream(existingNodes);
      ObjectInputStream in = null;
      try {
        in = new ObjectInputStream(bis);
        // generic in java lose their runtime information, there is no way to get this casted without
        // the need for the SuppressWarnings on the method.
        return (T) in.readObject();
      } finally {
        try {
          bis.close();
        } finally {
          if (in != null) {
            in.close();
          }
        }
      }
    } else {
      return null;
    }
  }

  /**
   * Set serializable object in store.
   *
   * @throws ExecutionException
   * @throws InterruptedException
   * @throws IOException
   */
  private <T extends Object> void set(String key, T object) throws InterruptedException,
    ExecutionException, IOException {
    Variable value = zkState.fetch(key).get();
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    ObjectOutputStream out = null;
    try {
      out = new ObjectOutputStream(bos);
      out.writeObject(object);
      value = value.mutate(bos.toByteArray());
      zkState.store(value).get();
    } finally {
      try {
        if (out != null) {
          out.close();
        }
      } finally {
        bos.close();
      }
    }
  }
}
