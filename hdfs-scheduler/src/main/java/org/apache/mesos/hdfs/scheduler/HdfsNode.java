package org.apache.mesos.hdfs.scheduler;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.*;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.protobuf.CommandInfoBuilder;
import org.apache.mesos.protobuf.EnvironmentBuilder;
import org.apache.mesos.protobuf.ExecutorInfoBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * HdfsNode base class.
 */
public abstract class HdfsNode implements IOfferEvaluator, ILauncher {
  private final Log log = LogFactory.getLog(HdfsNode.class);
  private final ResourceBuilder resourceBuilder;

  protected final HdfsFrameworkConfig config;
  protected final HdfsState state;
  protected final String name;

  public HdfsNode(HdfsState state, HdfsFrameworkConfig config, String name) {
    this.state = state;
    this.config = config;
    this.name = name;
    this.resourceBuilder = new ResourceBuilder(config.getHdfsRole());
  }

  public String getName() {
    return name;
  }

  protected abstract String getExecutorName();

  protected abstract List<String> getTaskTypes();

  public void launch(SchedulerDriver driver, Offer offer)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    List<Task> tasks = createTasks(offer);
    List<TaskInfo> taskInfos = getTaskInfos(tasks);

    // The recording of Tasks is what can potentially throw the exceptions noted above.  This is good news
    // because we are guaranteed that we do not actually launch Tasks unless we have recorded them.
    recordTasks(tasks);
    driver.launchTasks(Arrays.asList(offer.getId()), taskInfos);
  }

  private List<TaskInfo> getTaskInfos(List<Task> tasks) {
    List<TaskInfo> taskInfos = new ArrayList<TaskInfo>();

    for (Task task : tasks) {
      taskInfos.add(task.getInfo());
    }

    return taskInfos;
  }

  private void recordTasks(List<Task> tasks)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    for (Task task : tasks) {
      state.recordTask(task);
    }
  }

  private ExecutorInfo createExecutor(String taskIdName, String nodeName, String nnNum, String executorName) {

    String cmd = "export JAVA_HOME=$MESOS_DIRECTORY/" + config.getJreVersion()
      + " && env ; cd hdfs-mesos-* && "
      + "exec `if [ -z \"$JAVA_HOME\" ]; then echo java; "
      + "else echo $JAVA_HOME/bin/java; fi` "
      + "$HADOOP_OPTS "
      + "$EXECUTOR_OPTS "
      + "-cp lib/*.jar org.apache.mesos.hdfs.executor." + executorName;

    return ExecutorInfoBuilder.createExecutorInfoBuilder()
      .setName(nodeName + " executor")
      .setExecutorId(ExecutorInfoBuilder.createExecutorId("executor." + taskIdName))
      .addAllResources(getExecutorResources())
      .setCommand(CommandInfoBuilder.createCmdInfo(cmd, getCmdUriList(nnNum), getExecutorEnvironment()))
      .build();
  }

  private List<CommandInfo.URI> getCmdUriList(String nnNum) {
    int confServerPort = config.getConfigServerPort();

    String url = String.format("http://%s:%d/%s", config.getFrameworkHostAddress(),
      confServerPort, HDFSConstants.HDFS_CONFIG_FILE_NAME);
    if (nnNum != null) {
      url += "?" + HDFSConstants.NAMENODE_NUM_PARAM + "=" + nnNum;
    }

    return Arrays.asList(
      CommandInfoBuilder.createCmdInfoUri(String.format("http://%s:%d/%s", config.getFrameworkHostAddress(),
        confServerPort,
        HDFSConstants.HDFS_BINARY_FILE_NAME)),
      CommandInfoBuilder.createCmdInfoUri(url),
      CommandInfoBuilder.createCmdInfoUri(config.getJreUrl()));
  }

  private List<Environment.Variable> getExecutorEnvironment() {
    return Arrays.asList(
      EnvironmentBuilder.createEnvironment("LD_LIBRARY_PATH", config.getLdLibraryPath()),
      EnvironmentBuilder.createEnvironment("EXECUTOR_OPTS", "-Xmx" + config.getExecutorHeap() + "m -Xms" +
        config.getExecutorHeap() + "m"));
  }

  private List<String> getTaskNames(String taskType) {
    List<String> names = new ArrayList<String>();

    try {
      List<Task> tasks = state.getTasks();
      for (Task task : tasks) {
        if (task.getType().equals(taskType)) {
          names.add(task.getName());
        }
      }
    } catch (Exception ex) {
      log.error("Failed to retrieve task names, with exception: " + ex);
    }

    return names;
  }

  private int getTaskTargetCount(String taskType) throws SchedulerException {
    switch (taskType) {
      case HDFSConstants.NAME_NODE_ID:
        return HDFSConstants.TOTAL_NAME_NODES;
      case HDFSConstants.JOURNAL_NODE_ID:
        return config.getJournalNodeCount();
      default:
        return 0;
    }
  }

  private List<Resource> getTaskResources(String taskType) {
    NodeConfig nodeConfig = config.getNodeConfig(taskType);
    double cpu = nodeConfig.getCpus();
    double mem = nodeConfig.getMaxHeap() * config.getJvmOverhead();

    List<Resource> resources = new ArrayList<Resource>();
    resources.add(resourceBuilder.createCpuResource(cpu));
    resources.add(resourceBuilder.createMemResource(mem));

    return resources;
  }


  private String getNextTaskName(String taskType) {
    int targetCount = getTaskTargetCount(taskType);
    for (int i = 1; i <= targetCount; i++) {
      Collection<String> nameNodeTaskNames = getTaskNames(taskType);
      String nextName = taskType + i;
      if (!nameNodeTaskNames.contains(nextName)) {
        return nextName;
      }
    }

    // If we are attempting to find a name for a node type that
    // expects more than 1 instance (e.g. namenode1, namenode2, etc.)
    // we should not reach here.
    if (targetCount > 0) {
      String errorStr = "Task name requested when no more names are available for Task type: " + taskType;
      log.error(errorStr);
      throw new SchedulerException(errorStr);
    }

    return taskType;
  }

  private List<Resource> getExecutorResources() {
    double cpu = config.getExecutorCpus();
    double mem = config.getExecutorHeap() * config.getJvmOverhead();

    return Arrays.asList(
      resourceBuilder.createCpuResource(cpu),
      resourceBuilder.createMemResource(mem));
  }

  protected boolean enoughResources(Offer offer, double cpus, int mem) {
    for (Resource offerResource : offer.getResourcesList()) {
      if (offerResource.getName().equals("cpus") &&
        cpus + config.getExecutorCpus() > offerResource.getScalar().getValue()) {
        return false;
      }

      if (offerResource.getName().equals("mem") &&
        (mem * config.getJvmOverhead())
          + (config.getExecutorHeap() * config.getJvmOverhead())
          > offerResource.getScalar().getValue()) {
        return false;
      }
    }

    return true;
  }

  private List<Task> createTasks(Offer offer) {
    String executorName = getExecutorName();
    String taskIdName = String.format("%s.%s.%d", name, executorName, System.currentTimeMillis());
    List<Task> tasks = new ArrayList<>();

    String nnNum = getTaskTypes().contains(HDFSConstants.NAME_NODE_ID)
      ? getNextTaskName(HDFSConstants.NAME_NODE_ID)
      : null;

    for (String type : getTaskTypes()) {
      String taskName = getNextTaskName(type);

      List<Resource> resources = getTaskResources(type);
      ExecutorInfo execInfo = createExecutor(taskIdName, name, nnNum, executorName);

      tasks.add(new Task(resources, execInfo, offer, taskName, type, taskIdName));
    }

    return tasks;
  }
}
