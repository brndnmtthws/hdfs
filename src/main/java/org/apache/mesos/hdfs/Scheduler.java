package org.apache.mesos.hdfs;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.state.ClusterState;
import org.apache.mesos.hdfs.state.State;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.state.ZooKeeperState;
import org.joda.time.DateTime;
import org.joda.time.Seconds;


public class Scheduler implements org.apache.mesos.Scheduler, Runnable {
    
  private static final String NAME_NODE_ID = "namenode";
  private static final String JOURNAL_NODE_ID = "journalnode";
  private static final String DATA_NODE_ID = "datanode";
  private static final String ZKFC_NODE_ID = "zkfc";
  private static final String NODE_EXECUTOR_ID = "NodeExecutor";
  private static final String PRIMARY_NAME_NODE_EXECUTOR_ID = "PrimaryNameNodeExecutor";
  private static final String SECONDARY_NAME_NODE_EXECUTOR_ID = "SecondaryNameNodeExecutor";
  public static final String ACTIVATE_MESSAGE = "activate";
  private static final Integer TOTAL_NAME_NODES = 2;
  private static final Integer TOTAL_JOURNAL_NODES = 3;
    
  public static final Log log = LogFactory.getLog(Scheduler.class);
  private final SchedulerConf conf;
  private final String localhost;
  private Map<OfferID, Offer> pendingOffers;
  private boolean frameworkInitialized = false;
  private boolean initializingCluster = false;
  private Set<TaskID> stagingTasks = new HashSet<>();
  //TODO(elingg) simplify number of variables used including variables related to NameNodes
  private boolean firstNameNodeLaunched = false;
  private int nameNodesRunning = 0;
  private int journalNodesRunning = 0;

  @Inject
  public Scheduler(SchedulerConf conf) {
    this.conf = conf;
    initClusterState();
    pendingOffers = new ConcurrentHashMap<>();

    try {
      localhost = InetAddress.getLocalHost().getHostAddress();
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }
    
  private void initClusterState() {
    MesosNativeLibrary.load(conf.getNativeLibrary());
    ZooKeeperState zkState = new ZooKeeperState(conf.getStateZkServers(),
        conf.getStateZkTimeout(), TimeUnit.MILLISECONDS, "/hdfs-mesos/" + conf.getClusterName());
    State state = new State(zkState);
    ClusterState clusterState = ClusterState.getInstance();
    clusterState.init(state);
  }

  public SchedulerConf getConf() {
    return conf;
  }

  @Override
  public void disconnected(SchedulerDriver driver) {
    log.info("Scheduler driver disconnected");
  }

  @Override
  public void error(SchedulerDriver driver, String message) {
    log.error("Scheduler driver error: " + message);
  }

  @Override
  public void executorLost(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      int status) {
    log.info("Executor lost: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " status=" + status);
  }

  @Override
  public void frameworkMessage(SchedulerDriver driver, ExecutorID executorID, SlaveID slaveID,
      byte[] data) {
    log.info("Framework message: executorId=" + executorID.getValue() + " slaveId="
        + slaveID.getValue() + " data='" + Arrays.toString(data) + "'");
  }

  @Override
  public void offerRescinded(SchedulerDriver driver, OfferID offerId) {
    log.info("Offer rescinded: offerId=" + offerId.getValue());
  }

  @Override
  public void registered(SchedulerDriver driver, FrameworkID frameworkId, MasterInfo masterInfo) {
    try {
      ClusterState clusterState = ClusterState.getInstance();
      clusterState.getState().setFrameworkId(frameworkId);
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
    log.info("Registered framework frameworkId=" + frameworkId.getValue());
  }

  @Override
  public void reregistered(SchedulerDriver driver, MasterInfo masterInfo) {
    log.info("Reregistered framework.");
    ClusterState clusterState = ClusterState.getInstance();
    clusterState.clear();
  }

  private void launchNode(SchedulerDriver driver, Offer offer,
      String nodeName, List<String> taskNames, String executorName) {
    ClusterState clusterState = ClusterState.getInstance();
    log.info(String.format("Launching node of type %s with tasks %s", nodeName,
        taskNames.toString()));
    int confServerPort = conf.getConfigServerPort();
    List<Resource> resources = getExecutorResources();
    String taskIdName = String.format("%s.%s.%d", nodeName, executorName,
        System.currentTimeMillis());

    ExecutorInfo executorInfo = ExecutorInfo.newBuilder()
        .setName(nodeName + " executor")
        .setExecutorId(ExecutorID.newBuilder().setValue("executor." + taskIdName).build())
        .addAllResources(resources)
        .setCommand(CommandInfo.newBuilder()
            .addAllUris(Arrays.asList(
                CommandInfo.URI.newBuilder().setValue(conf.getExecUri())
                    .build(),
                CommandInfo.URI.newBuilder().setValue(
                    String.format("http://%s:%d/hdfs-site.xml", localhost, confServerPort))
                    .build()))
            .setEnvironment(Environment.newBuilder()
                .addAllVariables(Arrays.asList(
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_OPTS")
                        .setValue(conf.getJvmOpts()).build(),
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_HEAPSIZE")
                        .setValue(String.format("%d", conf.getHadoopHeapSize())).build(),
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_NAMENODE_OPTS")
                        .setValue("-Xmx" + conf.getNameNodeHeapSize() + "m").build(),
                    Environment.Variable.newBuilder()
                        .setName("HADOOP_DATANODE_OPTS")
                        .setValue("-Xmx" + conf.getDataNodeHeapSize() + "m").build(),
                    Environment.Variable.newBuilder()
                        .setName("EXECUTOR_OPTS")
                        .setValue("-Xmx" + conf.getExecutorHeap() + "m").build())))
            .setValue("env ; cd hadoop-2.* && exec java $HADOOP_OPTS $EXECUTOR_OPTS " +
                      "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName)
            .build())
        .build();

    List<TaskInfo> tasks = new ArrayList<>();
    for (String taskName : taskNames) {
      List<Resource> taskResources = getTaskResources(taskName);
      TaskID taskId = TaskID.newBuilder()
          .setValue(String.format("task.%s.%s", taskName, taskIdName))
          .build();
      TaskInfo task = TaskInfo.newBuilder()
          .setExecutor(executorInfo)
          .setName(taskName)
          .setTaskId(taskId)
          .setSlaveId(offer.getSlaveId())
          .addAllResources(taskResources)
          .setData(ByteString.copyFromUtf8(
              String.format("bin/hdfs-mesos-%s", taskName)))
          .build();
      tasks.add(task);

      stagingTasks.add(taskId);
      clusterState.addTask(taskId,
          new DfsTask(taskName, offer.getSlaveId().getValue(), offer.getHostname()));
    }
    driver.launchTasks(Arrays.asList(offer.getId()), tasks);
  }
    
  private List<Resource> getExecutorResources() {
    return Arrays.asList(
        Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getExecutorCpus()).build())
            .setRole("*")
            .build(),
         Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getExecutorHeap() * conf.getJvmOverhead()).build())
            .setRole("*")
            .build());
  }
    
  private List<Resource> getTaskResources(String taskName) {
    return Arrays.asList(
        Resource.newBuilder()
            .setName("cpus")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getTaskCpus(taskName)).build())
            .setRole("*")
            .build(),
        Resource.newBuilder()
            .setName("mem")
            .setType(Value.Type.SCALAR)
            .setScalar(Value.Scalar.newBuilder()
                .setValue(conf.getTaskHeapSize(taskName)).build())
            .setRole("*")
            .build());
  }

  private void launchNameNode(SchedulerDriver driver, Offer offer) {
    if (!firstNameNodeLaunched) {
      launchNode(driver, offer, NAME_NODE_ID,
          Arrays.asList(NAME_NODE_ID, ZKFC_NODE_ID, JOURNAL_NODE_ID),
              PRIMARY_NAME_NODE_EXECUTOR_ID);
      firstNameNodeLaunched = true;
    } else {
      launchNode(driver, offer, NAME_NODE_ID,
          Arrays.asList(NAME_NODE_ID, ZKFC_NODE_ID, JOURNAL_NODE_ID),
              SECONDARY_NAME_NODE_EXECUTOR_ID);
    }
  }

  private void launchJournalNode(SchedulerDriver driver, Offer offer) {
    launchNode(driver, offer, JOURNAL_NODE_ID, Arrays.asList(JOURNAL_NODE_ID), NODE_EXECUTOR_ID);
  }

  private void launchDataNode(SchedulerDriver driver, Offer offer) {
    launchNode(driver, offer, DATA_NODE_ID, Arrays.asList(DATA_NODE_ID), NODE_EXECUTOR_ID);
  }

  private void launchInitialJournalNodes(SchedulerDriver driver, Collection<Offer> offers) {
    int journalNodes = 0;
    for (Offer offer : offers) {
      pendingOffers.remove(offer.getId());
      launchJournalNode(driver, offer);
      journalNodes++;
      if (journalNodes >= conf.getJournalNodeCount()) {
        return;
      }
    }
  }

  private void launchInitialNameNodes(SchedulerDriver driver, Collection<Offer> offers) {
    int nameNodes = 0;
    // Find offer that matches
    for (Offer offer : offers) {
        pendingOffers.remove(offer.getId());
        launchNameNode(driver, offer);
        nameNodes++;
        if (nameNodes >= TOTAL_NAME_NODES) {
          return;
        }
      }
  }

  @Override
  synchronized public void resourceOffers(SchedulerDriver driver, List<Offer> offers) {
    //TODO(elingg) all datanodes can be launched together after the other nodes have initialized.
    //Remove this waiting period for datanodes.
    log.info(String.format("Received %d offers", offers.size()));
    if (!stagingTasks.isEmpty()) {
      log.info("Declining offers because tasks are currently staging");
      for (Offer offer : offers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }

    ClusterState clusterState = ClusterState.getInstance();
    if (clusterState.getNameNodes().size() == 0 && clusterState.getJournalNodes().size() == 0) {
      // Cluster must be formatted! Looks like we're starting fresh?
      log.info("No NameNodes or JournalNodes found.  Collecting offers until we have sufficient"
          + "capacity to launch.");

      int nameNodes = 0;
      int journalNodes = 0;
      List<Offer> incomingOffers = new ArrayList<>();
      incomingOffers.addAll(offers);
      incomingOffers.addAll(pendingOffers.values());
      for (Offer offer : incomingOffers) {
        if (nameNodes < TOTAL_NAME_NODES
            && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
              nameNodes++;
              pendingOffers.put(offer.getId(), offer);
        } else if (journalNodes < conf.getJournalNodeCount()
            && clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
          journalNodes++;
          pendingOffers.put(offer.getId(), offer);
        } else {
          driver.declineOffer(offer.getId());
        }
      }
      log.info(String.format(
          "Currently have %d pending offers, with journalnodes=%d and namenodes=%d",
          pendingOffers.size(), journalNodes, nameNodes));
      if (!initializingCluster && nameNodes == TOTAL_NAME_NODES
            && journalNodes >= conf.getJournalNodeCount()) {
        log.info("Launching initial nodes with pending offers");
        initializingCluster = true;
        launchInitialJournalNodes(driver, pendingOffers.values());
        launchInitialNameNodes(driver, pendingOffers.values());
        // Decline any remaining offer
        for (OfferID offerID : pendingOffers.keySet()) {
          driver.declineOffer(offerID);
        }
        pendingOffers.clear();
      }
      return;
    }

    List<Offer> remainingOffers = new ArrayList<>();
    remainingOffers.addAll(offers);

    if (initializingCluster) {
      log.info(String.format("Declining remaining %d offers pending initialization",
          remainingOffers.size()));
      for (Offer offer : remainingOffers) {
        driver.declineOffer(offer.getId());
      }
      return;
    }

    offers = remainingOffers;
    remainingOffers = new ArrayList<>();

    // Check to see if we can launch some DataNodes
    for (Offer offer : offers) {
      if (clusterState.notInDfsHosts(offer.getSlaveId().getValue())) {
        launchDataNode(driver, offer);
      } else {
        remainingOffers.add(offer);
      }
    }

    // Decline remaining offers
    log.info(String.format("Declining %d offers", remainingOffers.size()));
    for (Offer offer : remainingOffers) {
      driver.declineOffer(offer.getId());
    }
  }

  @Override
  public void slaveLost(SchedulerDriver driver, SlaveID slaveId) {
    log.info("Slave lost slaveId=" + slaveId.getValue());
  }


  @Override
  public void statusUpdate(SchedulerDriver driver, TaskStatus status) {
    log.info(String.format(
        "Received status update for taskId=%s state=%s message='%s' stagingTasks.size=%d",
            status.getTaskId().getValue(),
            status.getState().toString(),
            status.getMessage(),
            stagingTasks.size()));
    
    ClusterState clusterState = ClusterState.getInstance();
    DfsTask dfsTask = clusterState.getDfsTask(status.getTaskId());
    List<TaskID> currentStagingTasksList = new ArrayList<TaskID>();
    currentStagingTasksList.addAll(stagingTasks);

    if (status.getState().equals(TaskState.TASK_FAILED)
        || status.getState().equals(TaskState.TASK_FINISHED)
        || status.getState().equals(TaskState.TASK_KILLED)
        || status.getState().equals(TaskState.TASK_LOST)) {
      stagingTasks.remove(status.getTaskId());
      clusterState.removeTask(status);
    } else if (status.getState().equals(TaskState.TASK_RUNNING)) {
        stagingTasks.remove(status.getTaskId());
        clusterState.updateTask(status);
        //TODO(elingg) get rid of this extra variable and also DFS task object.  Instead use cluster
        //state or a cleaner way of keeping track of what is running. Get rid of the
        //namenodesInitialized variable.
        if (dfsTask.type == DfsTask.Type.NN) {
          nameNodesRunning++;
          if (nameNodesRunning == TOTAL_NAME_NODES) {
            //Finished initializing cluster after both name nodes are initialized
            initializingCluster = false;
          } else {
            //Activate secondary name node after first name node is activated
            for (TaskID taskId : currentStagingTasksList) {
              if (taskId.getValue().contains(SECONDARY_NAME_NODE_EXECUTOR_ID)) {
                sendMessageTo(driver, taskId, ACTIVATE_MESSAGE);
                break;
              }
            }
          }
        } else if (dfsTask.type == DfsTask.Type.JN) {
          journalNodesRunning++;
          if (journalNodesRunning == TOTAL_JOURNAL_NODES) {
            //Activate primary name node after all journal nodes are activated
              for (TaskID taskId : currentStagingTasksList) {
                if (taskId.getValue().contains(PRIMARY_NAME_NODE_EXECUTOR_ID)) {
                  sendMessageTo(driver, taskId, ACTIVATE_MESSAGE);
                  break;
                }
              }
           }
        }
     }
  }

  @Override
  public void run() {
    ClusterState clusterState = ClusterState.getInstance();
    FrameworkInfo.Builder frameworkInfo = FrameworkInfo.newBuilder()
        .setName("HDFS " + conf.getClusterName())
        .setFailoverTimeout(conf.getFailoverTimeout())
        .setUser(conf.getHdfsUser())
        .setRole(conf.getHdfsRole())
        .setCheckpoint(true);

    try {
      FrameworkID frameworkID = clusterState.getState().getFrameworkID();
      if (frameworkID != null) {
        frameworkInfo.setId(frameworkID);
        frameworkInitialized = true;
      }
    } catch (InterruptedException | ExecutionException | InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }

    MesosSchedulerDriver driver = new MesosSchedulerDriver(this, frameworkInfo.build(),
        conf.getMesosMasterUri());
    driver.run().getValueDescriptor().getFullName();
  }
    
  private void sendMessageTo(SchedulerDriver driver, TaskID taskId, String message) {
    ClusterState clusterState = ClusterState.getInstance();
    log.info(String.format("Sending message '%s' to taskId=%s", message, taskId.getValue()));
    DfsTask dfsTask = clusterState.getDfsTask(taskId);
    String postfix = taskId.getValue();
    postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
    postfix = postfix.substring(postfix.indexOf(".") + 1, postfix.length());
    driver.sendFrameworkMessage(
        ExecutorID.newBuilder().setValue("executor." + postfix).build(),
        SlaveID.newBuilder().setValue(dfsTask.slaveId).build(),
        message.getBytes());
  }

  public static class DfsTask {
    public final Type type;
    public TaskStatus taskStatus;
    public String slaveId;
    public String hostname;

    public DfsTask(String type, String slaveId, String hostname) {
      switch (type) {
        case NAME_NODE_ID :
          this.type = Type.NN;
          break;
        case JOURNAL_NODE_ID :
          this.type = Type.JN;
          break;
        case DATA_NODE_ID :
          this.type = Type.DN;
          break;
        case ZKFC_NODE_ID :
          this.type = Type.ZKFC;
          break;
        default :
          throw new RuntimeException("Invalid type: " + type);
      }
      this.slaveId = slaveId;

      // Make sure this is the actual hostname.
      try {
        InetAddress addr = InetAddress.getByName(hostname);
        this.hostname = addr.getHostAddress();
      } catch (UnknownHostException e) {
        log.error(e);
        this.hostname = hostname;
      }
    }

    public enum Type {
      NN,
      DN,
      JN,
      ZKFC,
    }
  }

}
