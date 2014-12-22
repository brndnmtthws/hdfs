package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.*;
import org.apache.mesos.hdfs.config.SchedulerConf;
import org.apache.mesos.hdfs.ProdConfigModule;

/**
 * The executor for the Secondary Name Node Machine.
 **/
public class SecondaryNameNodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(SecondaryNameNodeExecutor.class);

  private Task nameNodeTask;
  private Task zkfcNodeTask;
  private Task journalNodeTask;
  private int taskCount;

  /**
   * The constructor for the secondary name node which saves the configuration.
   **/
  @Inject
  SecondaryNameNodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new ProdConfigModule());
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(SecondaryNameNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks in the following order: : 1) Start Journal
   * Node 2) Receive Activate Message 3) Start Name Node 4) Start ZKFC Node
   **/
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    if (taskInfo.getTaskId().getValue().contains(JOURNAL_NODE_TASKID)) {
      journalNodeTask = task;
      // Add the zkfc node task and wait for activate message
      startProcess(driver, journalNodeTask);
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(journalNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
    } else if (taskInfo.getTaskId().getValue().contains(NAME_NODE_TASKID)) {
      // Add the name node task and wait for activate message
      nameNodeTask = task;
    } else if (taskInfo.getTaskId().getValue().contains(ZKFC_NODE_TASKID)) {
      // Add the zkfc node task and wait for activate message
      zkfcNodeTask = task;
    }

  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    Task task = null;
    if (taskId.getValue().contains(JOURNAL_NODE_TASKID)) {
      task = journalNodeTask;
    } else if (taskId.getValue().contains(NAME_NODE_TASKID)) {
      task = nameNodeTask;
    } else if (taskId.getValue().contains(ZKFC_NODE_TASKID)) {
      task = zkfcNodeTask;
    }
    if (task != null && task.process != null) {
      task.process.destroy();
      task.process = null;
    }
  }

  @Override
  public void frameworkMessage(ExecutorDriver driver, byte[] msg) {
    log.info("Executor received framework message of length: " + msg.length + " bytes");
    String messageStr = new String(msg);
    if (messageStr.equals("activate")) {
      // Bootstrap the secondary name node
      runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode -b");
      // Start the secondary name node
      startProcess(driver, nameNodeTask);
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(nameNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      // Start the zkfc node
      startProcess(driver, zkfcNodeTask);
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(zkfcNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
    }
  }

}
