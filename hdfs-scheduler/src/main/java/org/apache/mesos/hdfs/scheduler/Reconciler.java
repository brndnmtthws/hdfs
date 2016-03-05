package org.apache.mesos.hdfs.scheduler;

import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.TaskStatusFactory;
import org.apache.mesos.protobuf.TaskUtil;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * HDFS Mesos Framework Reconciler class implementation.
 */
public class Reconciler implements Observer {
  private final Log log = LogFactory.getLog(HdfsScheduler.class);

  private HdfsFrameworkConfig config;
  private HdfsState state;
  private Set<String> pendingTasks;

  @Inject
  public Reconciler(HdfsFrameworkConfig config, HdfsState state) {
    this.config = config;
    this.state = state;
    this.pendingTasks = new HashSet<String>();
  }

  public void reconcile(SchedulerDriver driver) throws InterruptedException, ExecutionException {
    pendingTasks = state.getTaskIds();
    (new ReconcileThread(this, driver)).start();
  }

  private void reconcileInternal(SchedulerDriver driver) {
    if (pendingTasks != null) {
      logPendingTasks();
      explicitlyReconcileTasks(driver);
    } else {
      log.warn("IPersistentStateStore returned null list of TaskIds");
    }

    implicitlyReconcileTasks(driver);
  }

  public void update(Observable obs, Object obj) {
    TaskStatus status = (TaskStatus) obj;

    String taskId = status.getTaskId().getValue();
    log.info("Received task update for: " + taskId);

    if (!complete()) {
      log.info("Reconciliation is NOT complete");

      if (taskIsPending(taskId)) {
        log.info(String.format("Reconciling Task '%s'.", taskId));
        pendingTasks.remove(taskId);
      } else {
        log.info(String.format("Task %s has already been reconciled or is unknown.", taskId));
      }

      logPendingTasks();

      if (complete()) {
        log.info("Reconciliation is complete");
      }
    }
  }

  private boolean taskIsPending(String taskId) {
    for (String t : pendingTasks) {
      if (t.equals(taskId)) {
        return true;
      }
    }

    return false;
  }

  public boolean complete() {
    if (pendingTasks.size() > 0) {
      return false;
    }

    return true;
  }

  private void logPendingTasks() {
    log.info("=========================================");
    log.info("pendingTasks size: " + pendingTasks.size());
    for (String t : pendingTasks) {
      log.info(t);
    }
    log.info("=========================================");
  }

  private void implicitlyReconcileTasks(SchedulerDriver driver) {
    log.info("Implicitly Reconciling Tasks");
    driver.reconcileTasks(Collections.<TaskStatus>emptyList());
  }

  private void explicitlyReconcileTasks(SchedulerDriver driver) {
    log.info("Explicitly Reconciling Tasks");
    List<TaskStatus> tasks = new ArrayList<TaskStatus>();

    for (String id : pendingTasks) {
      if (id == null) {
        log.warn("NULL TaskID encountered during Explicit Reconciliation.");
      } else {
        Protos.TaskID taskId = TaskUtil.createTaskId(id);

        TaskStatus taskStatus = TaskStatusFactory.createRunningStatus(taskId);
        tasks.add(taskStatus);
      }
    }

    driver.reconcileTasks(tasks);
  }

  private class ReconcileThread extends Thread {
    private static final int BACKOFF_MULTIPLIER = 2;

    private Reconciler reconciler;
    private SchedulerDriver driver;

    public ReconcileThread(Reconciler reconciler, SchedulerDriver driver) {
      this.reconciler = reconciler;
      this.driver = driver;
    }

    public void run() {
      int currDelay = reconciler.config.getReconciliationTimeout();

      while (!reconciler.complete()) {
        reconciler.reconcileInternal(driver);
        int sleepDuration = currDelay * HDFSConstants.MILLIS_FROM_SECONDS;

        log.info(String.format("Sleeping for %sms before retrying reconciliation.", sleepDuration));
        try {
          Thread.sleep(sleepDuration);
        } catch (InterruptedException ex) {
          log.warn(String.format("Reconciliation thread sleep was interrupted with exception: %s", ex));
        }

        currDelay = getDelay(currDelay);
      }
    }

    private int getDelay(int currDelay) {
      int tempDelay = currDelay * BACKOFF_MULTIPLIER;
      int maxDelay = reconciler.config.getMaxReconciliationTimeout();

      return Math.min(tempDelay, maxDelay);
    }
  }
}
