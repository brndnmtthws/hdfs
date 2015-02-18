package org.apache.mesos.hdfs.state;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.state.Variable;
import org.apache.mesos.state.ZooKeeperState;

import java.io.*;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PersistentState {
  private static String FRAMEWORK_ID_KEY = "frameworkId";
  private static String NAMENODES_KEY = "namenodes";
  private static String JOURNALNODES_KEY = "journalnodes";
  private static String DATANODES_KEY = "datanodes";
  private ZooKeeperState zkState;
  public static final Log log = LogFactory.getLog(PersistentState.class);

  public PersistentState(SchedulerConf conf) {
    MesosNativeLibrary.load(conf.getNativeLibrary());
    this.zkState = new ZooKeeperState(conf.getStateZkServers(),
        conf.getStateZkTimeout(), TimeUnit.MILLISECONDS, "/hdfs-mesos/" + conf.getClusterName());
  }

  public FrameworkID getFrameworkID() throws InterruptedException, ExecutionException,
      InvalidProtocolBufferException {
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

  // TODO (nicgrayson) add tests with in memory zookeeper
  public HashMap<String, String> getNamenodes() {
    return getHashMap(NAMENODES_KEY);
  }

  private void setNamenodes(HashMap<String, String> namenodes) {
    try {
      set(NAMENODES_KEY, namenodes);
    } catch (Exception e) {
      log.error("Error while setting namenodes in persistent state", e);
    }
  }

  public void addNode(Protos.TaskID taskId, String hostname, String taskName) {
    switch (taskName) {
      case HDFSConstants.NAME_NODE_ID :
        getNamenodes().put(taskId.getValue(), hostname);
        break;
      case HDFSConstants.JOURNAL_NODE_ID :
        getJournalnodes().put(taskId.getValue(), hostname);
        break;
      case HDFSConstants.DATA_NODE_ID :
        getDatanodes().put(taskId.getValue(), hostname);
        break;
      default :
        log.error("Task name unknown");
    }
  }

  public HashMap<String, String> getJournalnodes() {
    return getHashMap(JOURNALNODES_KEY);
  }

  private void setJournalnodes(HashMap<String, String> journalnodes) {
    try {
      set(JOURNALNODES_KEY, journalnodes);
    } catch (Exception e) {
      log.error("Error while setting journalnodes in persistent state", e);
    }
  }

  public HashMap<String, String> getDatanodes() {
    return getHashMap(DATANODES_KEY);
  }

  private void setDatanodes(HashMap<String, String> datanodes) {
    try {
      set(DATANODES_KEY, datanodes);
    } catch (Exception e) {
      log.error("Error while setting datanodes in persistent state", e);
    }
  }

  private HashMap<String, String> getHashMap(String key) {
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
  // public Node getNamenode() throws ClassNotFoundException, InterruptedException,
  // ExecutionException, IOException {
  // return get(NAMENODE_KEY);
  // }
  //
  // public void setNamenode(Node node) throws InterruptedException, ExecutionException, IOException
  // {
  // set(NAMENODE_KEY, node);
  // }
  //
  // public Set<Node> getNodes() throws InterruptedException, ExecutionException, IOException,
  // ClassNotFoundException {
  // Set<Node> nodes = get(NODES_KEY);
  // if (nodes == null)
  // nodes = new HashSet<Node>();
  // return nodes;
  // }
  //
  // public void setNodes(Set<Node> nodes) throws InterruptedException, ExecutionException,
  // IOException {
  // set(NODES_KEY, nodes);
  // }

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
   * Set serializable object in store
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

  // public static class Node implements Serializable {
  // private static final long serialVersionUID = 1L;
  //
  // public String hostname;
  // public String taskId;
  // public String slaveId;
  // }

}
