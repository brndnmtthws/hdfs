package org.apache.mesos.hdfs.executor;

import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.process.FailureUtils;
import org.apache.mesos.protobuf.TaskStatusBuilder;

/**
 * The executor for a Basic Node (either a Journal Node or Data Node).
 */
public class NodeExecutor extends AbstractNodeExecutor {
  private final Log log = LogFactory.getLog(NodeExecutor.class);
  private Task task;

  /**
   * The constructor for the node which saves the configuration.
   */
  @Inject
  NodeExecutor(HdfsFrameworkConfig config) {
    super(config);
  }

  /**
   * Main method for executor, which injects the configuration and state and starts the driver.
   */
  public static void main(String[] args) {
    Injector injector = Guice.createInjector();

    final NodeExecutor executor = injector.getInstance(NodeExecutor.class);
    MesosExecutorDriver driver = new MesosExecutorDriver(executor);
    Runtime.getRuntime().addShutdownHook(new Thread(new TaskShutdownHook(executor, driver)));
    FailureUtils.exit("mesos driver exited", driver.run() == Status.DRIVER_STOPPED ? 0 : 1);
  }

  /**
   * Add tasks to the task list and then start the tasks.
   */
  @Override
  public void launchTask(final ExecutorDriver driver, final TaskInfo taskInfo) {
    executorInfo = taskInfo.getExecutor();
    task = new Task(taskInfo);
    startProcess(driver, task);
    driver.sendStatusUpdate(TaskStatusBuilder.newBuilder()
      .setTaskId(taskInfo.getTaskId())
      .setState(TaskState.TASK_RUNNING)
      .setData(taskInfo.getData()).build());
  }

  @Override
  public void killTask(ExecutorDriver driver, TaskID taskId) {
    log.info("Killing task : " + taskId.getValue());
    if (task.getProcess() != null && taskId.equals(task.getTaskInfo().getTaskId())) {
      task.getProcess().destroy();
      task.setProcess(null);
    }
    driver.sendStatusUpdate(TaskStatusBuilder.newBuilder()
      .setTaskId(taskId)
      .setState(TaskState.TASK_KILLED)
      .build());
  }

  @Override
  public void shutdown(ExecutorDriver d) {
    // TODO(elingg) let's shut down the driver more gracefully
    log.info("Executor asked to shutdown");
    if (task != null) {
      killTask(d, task.getTaskInfo().getTaskId());
    }
  }
}
