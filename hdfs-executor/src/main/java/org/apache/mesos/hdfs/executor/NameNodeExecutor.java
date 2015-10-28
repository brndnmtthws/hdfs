package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.file.FileUtils;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.TaskStatusFactory;
import org.apache.mesos.process.FailureUtils;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.concurrent.TimeUnit;

/**
 * The Executor for NameNodes.
 */
public class NameNodeExecutor extends AbstractNodeExecutor {
  private final Log log = LogFactory.getLog(NameNodeExecutor.class);
  private final CuratorFramework curatorClient;

  private Task nameNodeTask;
  private Task zkfcNodeTask;

  /**
   * The constructor for the primary name node which saves the configuration.
   */
  @Inject
  NameNodeExecutor(HdfsFrameworkConfig config) {
    super(config);
    curatorClient = createCuratorClient();
  }

  private CuratorFramework createCuratorClient() {
    String hosts = config.getHaZookeeperQuorum();
    RetryPolicy retryPolicy =
      new ExponentialBackoffRetry(HDFSConstants.POLL_DELAY_MS, HDFSConstants.CURATOR_MAX_RETRIES);

    CuratorFramework client = CuratorFrameworkFactory.newClient(hosts, retryPolicy);
    client.start();
    return client;
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();
    final NameNodeExecutor executor = injector.getInstance(NameNodeExecutor.class);
    MesosExecutorDriver driver = new MesosExecutorDriver(executor);
    Runtime.getRuntime().addShutdownHook(new Thread(new TaskShutdownHook(executor, driver)));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Launches NameNode or ZKFC Nodes on request.
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();

    // NameNode Task
    if (taskInfo.getTaskId().getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      launchNameNodeTask(driver, taskInfo);

      TimedHealthCheck healthCheckNN = new TimedHealthCheck(driver, nameNodeTask);
      healthCheckTimer.scheduleAtFixedRate(healthCheckNN,
        config.getHealthCheckWaitingPeriod(),
        config.getHealthCheckFrequency());
      return;
    }

    // ZKFC Task
    if (taskInfo.getTaskId().getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      launchZKFCTask(driver, taskInfo);

      TimedHealthCheck healthCheckZN = new TimedHealthCheck(driver, zkfcNodeTask);
      healthCheckTimer.scheduleAtFixedRate(healthCheckZN,
        config.getHealthCheckWaitingPeriod(),
        config.getHealthCheckFrequency());
      return;
    }

    log.error("Unrecognized Task type attempting to launch: " + taskInfo);
  }

  private void launchNameNodeTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    Task task = new Task(taskInfo);
    log.info("Launching NameNode Task: " + task);
    nameNodeTask = task;

    // The actual starting of a NameNode requires that its DNS address be resolvable 
    // before it starts.  However, Mesos-DNS won't assign an address until the task is
    // listed as RUNNING.  So we start the Thread which is going to wait for the DNS
    // address.  We then "lie" to Mesos below telling it that the Task is RUNNING so we
    // can get a DNS address.
    Runnable r = new Runnable() {
      public void run() {
        try {
          initNameNode(driver, nameNodeTask.getTaskInfo().getName() + ".hdfs.mesos");
        } catch (Exception ex) {
          log.error("Failed to launch " + nameNodeTask.getTaskInfo().getName(), ex);
          // Failure to start a NameNode on a NameNodeExecutor is catastrophic.
          FailureUtils.exit("Failed to launch Namenode", HDFSConstants.NAMENODE_EXIT_CODE);
        }
      }
    };

    new Thread(r).start();

    // Lie to Mesos.  Tell it the NameNode Task is running, but we track it's actual
    // "uninitialized" status in the labels.  The Scheduler depends on these labels
    // for determing when it should move to the next phase of its state machine.
    TaskStatus status = TaskStatusFactory.createNameNodeStatus(
      nameNodeTask.getTaskInfo().getTaskId(),
      false);

    log.info("Sending status update: " + status);
    driver.sendStatusUpdate(status);
  }

  private void launchZKFCTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    Task task = new Task(taskInfo);
    log.info("Launching ZKFC Task: " + task);
    zkfcNodeTask = task;
    initZKFCNode(driver);
  }

  private void initNameNode(ExecutorDriver driver, String dnsName) throws Exception {
    waitDnsResolution(dnsName);

    InterProcessMutex lock = new InterProcessMutex(curatorClient, getStatusPath());
    if (lock.acquire(HDFSConstants.ZK_MUTEX_ACQUIRE_TIMEOUT_SEC, TimeUnit.SECONDS)) {
      try {

        // In order to start a set of NameNodes, one must first be formatted.  Other
        // NameNodes then bootstrap off that node or others which have already bootstrapped.
        // All Namenoedes are started simultaneously.  Their startups are intentionally
        // serialized through the mutex acquired above.  The value stored in the Znode which
        // is used as a mutex indicates whether or not any NameNode has ever been formatted.
        // The first NameNode to acquire the mutex and find that no NameNode has ever been
        // formatted, formats itself.  All others bootstrap.
        String backupDir = config.getBackupDir();
        String status = getNameNodeStatus();
        log.info("Initializing NN, status=" + status + ", backupDir=" + backupDir);

        if (status == null || status.isEmpty())  {
          formatNameNode(driver);
          setNameNodeStatus("formatted");
        } else if (status.equals("formatted") || backupDir == null) {
          bootstrapNameNode(driver);
          setNameNodeStatus("bootstrapped");
        }
      } finally {
        lock.release();
      }
    } else {
      throw new Exception("Failed to initialize NameNode status.");
    }
  }

  private void initZKFCNode(ExecutorDriver driver) {
    if (!processRunning(zkfcNodeTask)) {
      startProcess(driver, zkfcNodeTask);
    }

    TaskStatus status = TaskStatusFactory.createRunningStatus(zkfcNodeTask.getTaskInfo().getTaskId());
    driver.sendStatusUpdate(status);
  }

  private String getNameNodeStatus() throws Exception {
    byte[] data = curatorClient.getData().forPath(getStatusPath());
    return data != null ? new String(data, "UTF-8") : null;
  }

  private void setNameNodeStatus(String status) throws Exception {
    curatorClient.setData().forPath(getStatusPath(), status.getBytes(Charset.forName("UTF-8")));
  }

  private void formatNameNode(ExecutorDriver driver) {
    startNameNode(driver, HDFSConstants.NAME_NODE_INIT_MESSAGE);
  }

  private void bootstrapNameNode(ExecutorDriver driver) {
    startNameNode(driver, HDFSConstants.NAME_NODE_BOOTSTRAP_MESSAGE);
  }

  private void startNameNode(ExecutorDriver driver, String startType) {
    initDir();
    runNameNodeCommand(driver, startType);

    if (!processRunning(nameNodeTask)) {
      startProcess(driver, nameNodeTask);
    }

    TaskStatus status = TaskStatusFactory.createNameNodeStatus(
      nameNodeTask.getTaskInfo().getTaskId(),
      true);

    log.info("Sending status update: " + status);
    driver.sendStatusUpdate(status);
  }

  private void runNameNodeCommand(ExecutorDriver driver, String cmd) {
    runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode " + cmd);
  }

  private boolean waitDnsResolution(String dnsName) {
    while (!dnsResolves(dnsName)) {
      log.info("Waiting for DNS resolution: " + dnsName);
      try {
        Thread.sleep(HDFSConstants.POLL_DELAY_MS);
      } catch (InterruptedException ex) {
        log.warn("DNS sleep interrupted.");
      }
    }

    log.info("DNS resolved: " + dnsName);
    return true;
  }

  private boolean dnsResolves(String dnsName) {
    // Short circuit since Mesos handles this otherwise
    if (!config.usingMesosDns()) {
      return true;
    }

    log.info("Resolving DNS for " + dnsName);
    try {
      InetAddress.getByName(dnsName);
      log.info("Successfully found " + dnsName);
      return true;
    } catch (SecurityException | IOException e) {
      log.warn("Couldn't resolve dnsName " + dnsName);
      return false;
    }
  }

  private String getStatusPath() {
    return "/hdfs-mesos/" + config.getFrameworkName() + "/name_node_status";
  }

  private void initDir() {
    File nameDir = new File(config.getDataDir() + "/name");
    FileUtils.deleteDirectory(nameDir);
    if (!nameDir.mkdirs()) {
      final String errorMsg = "unable to make directory: " + nameDir;
      log.error(errorMsg);
      throw new ExecutorException(errorMsg);
    }

    File backupDir = config.getBackupDir() != null
        ? new File(config.getBackupDir() + "/" + nameNodeTask.getTaskInfo().getName())
        : null;

    if (backupDir != null && !backupDir.exists() && !backupDir.mkdirs()) {
      final String errorMsg = "unable to make directory: " + backupDir;
      log.error(errorMsg);
      throw new ExecutorException(errorMsg);
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    Task task = null;
    if (taskId.getValue().contains(HDFSConstants.NAME_NODE_TASKID)) {
      task = nameNodeTask;
    } else if (taskId.getValue().contains(HDFSConstants.ZKFC_NODE_ID)) {
      task = zkfcNodeTask;
    }

    if (task != null && task.getProcess() != null) {
      task.getProcess().destroy();
      task.setProcess(null);
    }

    TaskStatus status = TaskStatusFactory.createKilledStatus(taskId);
    log.info("Sending status update: " + status);
    driver.sendStatusUpdate(status);
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    // TODO(elingg) let's shut down the driver more gracefully
    log.info("Executor asked to shutdown");
    if (nameNodeTask != null) {
      killTask(d, nameNodeTask.getTaskInfo().getTaskId());
    }
    if (zkfcNodeTask != null) {
      killTask(d, zkfcNodeTask.getTaskInfo().getTaskId());
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    super.frameworkMessage(driver, msg);
    String messageStr = new String(msg, Charset.defaultCharset());
    log.info(String.format("Received framework message: %s", messageStr));
  }

  private boolean processRunning(Task task) {
    boolean running = false;

    if (task.getProcess() != null) {
      try {
        task.getProcess().exitValue();
      } catch (IllegalThreadStateException e) {
        // throws exception if still running
        running = true;
      }
    }
    return running;
  }
}
