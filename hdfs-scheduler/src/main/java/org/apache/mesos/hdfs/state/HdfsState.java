package org.apache.mesos.hdfs.state;

import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.scheduler.StateFactory;
import org.apache.mesos.hdfs.scheduler.Task;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.protobuf.TaskStatusBuilder;
import org.apache.mesos.state.State;
import org.apache.mesos.state.Variable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Observable;
import java.util.Observer;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * Reads and Writes the persisted state of the HDFS Framework.
 */
public class HdfsState implements Observer {
  private final State taskState;
  private final State schedulerState;
  private final Log log = LogFactory.getLog(HdfsState.class);
  private final String zkTaskPath;
  private final String zkSchedulerPath;

  @Inject
  public HdfsState(HdfsFrameworkConfig config, StateFactory stateFactory) {
    String zkPath = "/hdfs-mesos/" + config.getFrameworkName();
    zkTaskPath = zkPath + "/tasks";
    zkSchedulerPath = zkPath + "/scheduler";

    taskState = stateFactory.create(zkTaskPath, config);
    schedulerState = stateFactory.create(zkSchedulerPath, config);

    // Hack to initialize the path for Tasks.  This allows better logic
    // around returning an empty list of elements when querying the 
    // persisted tasks.
    initializeTaskState();
  }

  private boolean taskStateInitialized() {
    try {
      // This will throw an exception if nothing has ever been added
      // to the tasks ZNode.
      taskState.names().get();
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

  private void initializeTaskState() {
    if (taskStateInitialized()) {
      return;
    }

    try {
      // Put something in to initialize the path.
      Variable var = taskState.fetch("init").get();
      var = var.mutate(new byte[1]);
      taskState.store(var).get();

      // Now remove it.
      taskState.expunge(var).get();
    } catch (Exception ex) {
      log.error("Failed to initialize taskState with exception: " + ex);
    }
  }

  public void setFrameworkId(FrameworkID id) throws IOException, InterruptedException, ExecutionException {
    Variable var = schedulerState.fetch(HDFSConstants.ZK_FRAMEWORK_ID_KEY).get();
    var = var.mutate(Serializer.serialize(id));
    schedulerState.store(var).get();
  }

  public FrameworkID getFrameworkId()
    throws ClassNotFoundException, ExecutionException, InterruptedException, IOException {
    Variable var = schedulerState.fetch(HDFSConstants.ZK_FRAMEWORK_ID_KEY).get();

    if (var == null || var.value() == null || var.value().length == 0) {
      return null;
    } else {
      Object obj = Serializer.deserialize(var.value());
      return (FrameworkID) obj;
    }
  }

  public void removeFrameworkId()
    throws ClassNotFoundException, ExecutionException, InterruptedException, IOException {
    Variable var = schedulerState.fetch(HDFSConstants.ZK_FRAMEWORK_ID_KEY).get();
    schedulerState.expunge(var).get();
  }

  public void recordTask(Task task)
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    Variable var = taskState.fetch(task.getId().getValue()).get();

    TaskStatus currStatus = null;
    try {
      Task currTask = (Task) Serializer.deserialize(var.value());
      currStatus = currTask.getStatus();
      log.info("Retrieved old status: " + currStatus);
    } catch (Exception ex) {
      log.warn("Deserialization failed (as expected when setting status for the first time) with exception: " + ex);
    }

    TaskStatus status = mergeStatuses(currStatus, task.getStatus());
    log.info("Setting new status: " + status);
    task.setStatus(status);

    byte[] taskBytes = Serializer.serialize(task);
    var = var.mutate(taskBytes);
    taskState.store(var).get();
  }

  private TaskStatus mergeStatuses(TaskStatus curr, TaskStatus next) throws ClassNotFoundException {
    if (curr == null || next == null || next.hasLabels()) {
      return next;
    }

    // If the old status has labels, but the new status doesn't,
    // update the new status with the old labels.  We don't want labels
    // being overwritten to empty.  This would break determinging when
    // NameNodes are initialized since that computation relies on labels.
    if (curr.hasLabels()) {
      return new TaskStatusBuilder(next).setLabels(curr.getLabels()).build();
    }

    return next;
  }

  public Set<String> getTaskIds() throws InterruptedException, ExecutionException {
    Set<String> ids = new HashSet<String>();

    Iterator<String> iter = taskState.names().get();
    while (iter.hasNext()) {
      ids.add(iter.next());
    }

    return ids;
  }

  public List<Task> getTasks() throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    Set<String> taskIds = getTaskIds();

    List<Task> tasks = new ArrayList<Task>();
    for (String taskId : taskIds) {
      Task task = getTask(taskId);
      tasks.add(task);
    }

    return tasks;
  }

  private List<Task> getTasks(String nameFilter)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    List<Task> inputTasks = getTasks();
    List<Task> outputTasks = new ArrayList<Task>();

    for (Task task : inputTasks) {
      if (task.getName().contains(nameFilter)) {
        outputTasks.add(task);
      }
    }

    return outputTasks;
  }

  public void update(Observable observable, Object obj) {
    TaskStatus newStatus = (TaskStatus) obj;
    String taskId = newStatus.getTaskId().getValue();

    try {
      Variable var = taskState.fetch(taskId).get();

      if (isTerminalState(newStatus)) {
        taskState.expunge(var).get();
      } else {
        Task task = (Task) Serializer.deserialize(var.value());
        TaskStatus oldStatus = task.getStatus();
        newStatus = mergeStatuses(oldStatus, newStatus);

        task.setStatus(newStatus);
        byte[] taskBytes = Serializer.serialize(task);
        var = var.mutate(taskBytes);
        taskState.store(var).get();
      }
    } catch (ClassNotFoundException | IOException | InterruptedException | ExecutionException ex) {
      log.error("Failed to update TaskStatus with ID: " + taskId + "with exception: " + ex.getMessage());
    }
  }

  public boolean hostOccupied(String hostname, String taskType) {
    try {
      for (Task task : getTasks()) {
        if (task.getHostname().equals(hostname) && task.getType().equals(taskType)) {
          return true;
        }
      }
    } catch (Exception ex) {
      log.error(
        String.format("Failed to determine whether host: '%s' is occupied by Task type: '%s' with exception: %s",
          hostname,
          taskType,
          ex));
    }

    return false;
  }

  private Task getTask(String taskId)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    Variable var = taskState.fetch(taskId).get();
    return (Task) Serializer.deserialize(var.value());
  }

  private boolean isTerminalState(TaskStatus taskStatus) {
    return taskStatus.getState().equals(TaskState.TASK_FAILED)
      || taskStatus.getState().equals(TaskState.TASK_FINISHED)
      || taskStatus.getState().equals(TaskState.TASK_KILLED)
      || taskStatus.getState().equals(TaskState.TASK_LOST)
      || taskStatus.getState().equals(TaskState.TASK_ERROR);
  }

  public int getJournalCount()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    return getJournalNodeTasks().size();
  }

  public int getNameCount()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    return getNameNodeTasks().size();
  }

  public List<Task> getNameNodeTasks()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    return getTasks(HDFSConstants.NAME_NODE_ID);
  }

  public List<Task> getJournalNodeTasks()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    return getTasks(HDFSConstants.JOURNAL_NODE_ID);
  }

  public boolean nameNodesInitialized() {
    List<Task> tasks = null;
    try {
      tasks = getNameNodeTasks();
    } catch (Exception ex) {
      log.error("Failed to determine whether NameNodes are initialized with exception: " + ex);
      return false;
    }

    int initCount = 0;
    for (Task task : tasks) {
      TaskStatus status = task.getStatus();
      if (status != null) {
        for (Label label : status.getLabels().getLabelsList()) {
          String key = label.getKey();
          String value = label.getValue();
          if (key.equals(HDFSConstants.NN_STATUS_KEY) &&
            value.equals(HDFSConstants.NN_STATUS_INIT_VAL)) {
            initCount++;
          }
        }
      }
    }

    log.info(String.format("%s/%s NameNodes initialized.", initCount, HDFSConstants.TOTAL_NAME_NODES));
    return initCount == HDFSConstants.TOTAL_NAME_NODES;
  }
}
