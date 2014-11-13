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

import java.io.IOException;

/**
 * The executor for the Primary Name Node Machine.
 **/
public class PrimaryNameNodeExecutor extends AbstractNodeExecutor {
  public static final Log log = LogFactory.getLog(PrimaryNameNodeExecutor.class);

  private Task nameNodeTask;
  private Task zkfcNodeTask;
  private Task journalNodeTask;
  private int taskCount;

  /**
   * The constructor for the primary name node which saves the configuration.
   **/
  @Inject
  PrimaryNameNodeExecutor(SchedulerConf schedulerConf) {
    super(schedulerConf);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector(new ProdConfigModule());
    MesosExecutorDriver driver = new MesosExecutorDriver(
        injector.getInstance(PrimaryNameNodeExecutor.class));
    System.exit(driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks in the following order : 1) Start Journal
   * Node 2) Start Name Node 3) Start ZKFC Node
   **/
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    Task task = new Task(taskInfo);
    if (taskInfo.getTaskId().getValue().contains(".journalnode.")) {
      journalNodeTask = task;
    } else if (taskInfo.getTaskId().getValue().contains(".namenode.namenode.")) {
      nameNodeTask = task;
    } else if (taskInfo.getTaskId().getValue().contains(".zkfc.")) {
      zkfcNodeTask = task;
    }
    taskCount++;

    if (taskCount == 3) {
      // Start journal node
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(journalNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      startProcess(driver, journalNodeTask);
      // Initialize the journal node and name node
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(nameNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      runCommand(driver, nameNodeTask, "bin/hdfs-mesos-namenode -i");
      // Start the primary name node
      startProcess(driver, nameNodeTask);
      // Start the zkfc node
      driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(zkfcNodeTask.taskInfo.getTaskId())
          .setState(TaskState.TASK_RUNNING)
          .build());
      startProcess(driver, zkfcNodeTask);
    }
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    Task task = null;
    if (taskId.getValue().contains(".journalnode.")) {
      task = journalNodeTask;
    } else if (taskId.getValue().contains(".namenode.namenode.")) {
      task = nameNodeTask;
    } else if (taskId.getValue().contains(".zkfc.")) {
      task = zkfcNodeTask;
    }

    if (task != null && task.process != null) {
      task.process.destroy();
      task.process = null;
    }
  }

}
