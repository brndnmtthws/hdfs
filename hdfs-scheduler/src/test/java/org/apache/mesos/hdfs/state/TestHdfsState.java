package org.apache.mesos.hdfs.state;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.hdfs.TestSchedulerModule;
import org.apache.mesos.hdfs.scheduler.Task;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.hdfs.util.StatusFactory;
import org.junit.Test;

import java.io.IOException;
import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.*;

public class TestHdfsState {
  private final Injector injector = Guice.createInjector(new TestSchedulerModule());
  private SecureRandom random = new SecureRandom();
  private static final String testIdName = "framework";
  private static final String TEST_HOST = "host";
  private static final String TEST_TYPE = "type";
  private static final String TEST_NAME = "name";


  @Test
  public void testTerminalStatusUpdate()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    Task inTask = createTask();
    state.recordTask(inTask);

    TaskStatus status = createTaskStatus(inTask.getId(), TaskState.TASK_FAILED);
    state.update(null, status);
    List<Task> tasks = state.getTasks();
    assertEquals(0, tasks.size());
  }

  @Test
  public void testNonTerminalStatusUpdate()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    Task inTask = createTask();
    state.recordTask(inTask);

    TaskStatus status = createTaskStatus(inTask.getId(), TaskState.TASK_RUNNING);
    state.update(null, status);
    List<Task> tasks = state.getTasks();
    assertEquals(1, tasks.size());

    Task outTask = tasks.get(0);
    assertEquals(status, outTask.getStatus());
  }

  @Test
  public void testHostOccupied()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = createDefaultState();
    assertFalse(state.hostOccupied("wrong_host", TEST_TYPE));
    assertFalse(state.hostOccupied(TEST_HOST, "wrong_type"));
    assertFalse(state.hostOccupied("wrong_host", "wrong_type"));
    assertTrue(state.hostOccupied(TEST_HOST, TEST_TYPE));
  }

  @Test
  public void testGetNameNodeTasks()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    Task inTask = createNameNodeTask();
    state.recordTask(inTask);

    List<Task> nameTasks = state.getNameNodeTasks();
    assertEquals(1, nameTasks.size());

    List<Task> journalTasks = state.getJournalNodeTasks();
    assertEquals(0, journalTasks.size());
  }

  @Test
  public void testGetJournalNodeTasks()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    Task inTask = createJournalNodeTask();
    state.recordTask(inTask);

    List<Task> journalTasks = state.getJournalNodeTasks();
    assertEquals(1, journalTasks.size());

    List<Task> nameTasks = state.getNameNodeTasks();
    assertEquals(0, nameTasks.size());
  }

  @Test
  public void testNameNodesInitialized()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    assertFalse(state.nameNodesInitialized());

    Task namenode1Task = createNameNodeTask();
    Task namenode2Task = createNameNodeTask();
    state.recordTask(namenode1Task);
    state.recordTask(namenode2Task);

    TaskStatus status1 = StatusFactory.createNameNodeStatus(namenode1Task.getId(), true);
    TaskStatus status2 = StatusFactory.createNameNodeStatus(namenode2Task.getId(), true);

    state.update(null, status1);
    assertFalse(state.nameNodesInitialized());

    state.update(null, status2);
    assertTrue(state.nameNodesInitialized());
  }

  private HdfsState createDefaultState()
    throws ClassNotFoundException, IOException, InterruptedException, ExecutionException {
    HdfsState state = injector.getInstance(HdfsState.class);
    Task inTask = createTask();
    state.recordTask(inTask);
    return state;
  }

  private Task createTask() {
    return createTask(TEST_NAME);
  }

  private Task createNameNodeTask() {
    return createTask(HDFSConstants.NAME_NODE_ID);
  }

  private Task createJournalNodeTask() {
    return createTask(HDFSConstants.JOURNAL_NODE_ID);
  }

  private Task createTask(String name) {
    List<Resource> resources = createResourceList();
    ExecutorInfo execInfo = createExecutorInfo();
    Offer offer = createOffer();
    String taskIdName = createTaskIdName();
    return new Task(resources, execInfo, offer, name, TEST_TYPE, taskIdName);
  }

  public String createTaskIdName() {
    return "taskIdName_" + new BigInteger(130, random).toString(32);
  }

  private FrameworkID createFrameworkId() {
    return FrameworkID.newBuilder().setValue(testIdName).build();
  }

  private List<Resource> createResourceList() {
    Resource r = Resource.newBuilder()
      .setName("name")
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder()
        .setValue(1).build())
      .setRole("role")
      .build();

    List<Resource> resources = new ArrayList<Resource>();
    resources.add(r);
    return resources;
  }

  private TaskStatus createTaskStatus(TaskID taskID, TaskState state) {
    return TaskStatus.newBuilder()
      .setTaskId(taskID)
      .setState(state)
      .setSlaveId(SlaveID.newBuilder().setValue("slave").build())
      .setMessage("From Test")
      .build();
  }

  private TaskStatus createTaskStatusWithLabel(TaskID taskID, TaskState state, String value) {
    TaskStatus.Builder builder = TaskStatus.newBuilder(createTaskStatus(taskID, state));
    return builder.setLabels(Labels.newBuilder()
      .addLabels(Label.newBuilder()
        .setKey(HDFSConstants.NN_STATUS_KEY)
        .setValue(value)))
      .build();
  }

  private ExecutorInfo createExecutorInfo() {
    return ExecutorInfo
      .newBuilder()
      .setExecutorId(ExecutorID.newBuilder().setValue("executor").build())
      .setCommand(
        CommandInfo
          .newBuilder()
          .addAllUris(
            Arrays.asList(
              CommandInfo.URI
                .newBuilder()
                .setValue("http://test_url/")
                .build())))
      .build();
  }

  private Offer createOffer() {
    return Offer.newBuilder()
      .setId(createTestOfferId(1))
      .setFrameworkId(FrameworkID.newBuilder().setValue("framework").build())
      .setSlaveId(SlaveID.newBuilder().setValue("slave").build())
      .setHostname(TEST_HOST)
      .build();
  }

  private OfferID createTestOfferId(int instanceNumber) {
    return OfferID.newBuilder().setValue("offer" + instanceNumber).build();
  }
}
