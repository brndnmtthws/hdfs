package org.apache.mesos.hdfs.state

import com.google.inject.Guice
import org.apache.mesos.Protos
import org.apache.mesos.hdfs.TestSchedulerModule
import org.apache.mesos.hdfs.scheduler.Task
import org.apache.mesos.hdfs.util.HDFSConstants
import spock.lang.Shared
import spock.lang.Specification

import java.security.SecureRandom

/**
 *
 *
 */
class HdfsStateSpec extends Specification {
  def injector = Guice.createInjector(new TestSchedulerModule())
  static final String TEST_HOST = "host"
  static final String TEST_TYPE = "type"
  static final String TEST_NAME = "name"
  static final String testIdName = "framework"

  @Shared
  SecureRandom random = new SecureRandom()

  def "setting frameworkID"() {
    given:
    def state = injector.getInstance(HdfsState.class)
    def expectedId = createFrameworkId()
    state.setFrameworkId(expectedId)

    expect:
    expectedId == state.getFrameworkId()
  }

  def "retrieves the same record from storage"() {
    given:
    def state = injector.getInstance(HdfsState.class);
    def tasks = state.getTasks();

    expect:
    tasks.size() == 0

    when:
    def inTask = createTask(TEST_NAME);
    state.recordTask(inTask);
    tasks = state.getTasks();

    then:
    tasks.size() == 1
    with(tasks.get(0)) { task ->
      task.status == inTask.status
      task.offer == inTask.offer
      task.type == inTask.type
      task.name == inTask.name
    }
  }

  def "update label none to label task"() {
    given:
    HdfsState state = injector.getInstance(HdfsState.class)
    Task task1 = createTask(TEST_NAME)

    def status = createTaskStatusWithLabel(task1.id, Protos.TaskState.TASK_RUNNING, "value")
    task1.status = status
    state.recordTask(task1)

    when:
    state.update(null, status)

    then:
    state.getTasks().get(0).status == status
  }


  def "update label to label task"() {
    given:
    HdfsState state = injector.getInstance(HdfsState.class)
    Task task1 = createTask(TEST_NAME)

    def status1 = createTaskStatusWithLabel(task1.getId(), Protos.TaskState.TASK_RUNNING, "value1")
    def status2 = createTaskStatusWithLabel(task1.getId(), Protos.TaskState.TASK_RUNNING, "value2")
    task1.status = status1
    state.recordTask(task1)

    when:
    state.update(null, status2)

    then:
    Task outTask = state.getTasks().get(0)
    outTask.status == status2
  }

  def "update label tasks"() {
    given:
    HdfsState state = injector.getInstance(HdfsState.class)
    Task task1 = createTask(TEST_NAME)

    def status1 = createTaskStatusWithLabel(task1.getId(), Protos.TaskState.TASK_RUNNING, value1)
    def status2 = createTaskStatusWithLabel(task1.getId(), Protos.TaskState.TASK_RUNNING, value2)
    task1.status = status1
    state.recordTask(task1)

    when:
    state.update(null, status2)

    then:
    Task outTask = state.getTasks().get(0)
    outTask.status == status1Valid ? status1 : status2

    where:
    value1   | value2   | status1Valid
    "value1" | "value2" | false
    "value1" | null     | true
  }


  def createTaskStatusWithLabel(Protos.TaskID taskID, Protos.TaskState state, String value) {
    def status = createTaskStatus(taskID, state)
    if (!value) {
      return status
    }
    def builder = Protos.TaskStatus.newBuilder(status)
    return builder.setLabels(Protos.Labels.newBuilder()
      .addLabels(Protos.Label.newBuilder()
      .setKey(HDFSConstants.NN_STATUS_KEY)
      .setValue(value)))
      .build()
  }

  def createTaskStatus(Protos.TaskID taskID, Protos.TaskState state) {
    return Protos.TaskStatus.newBuilder()
      .setTaskId(taskID)
      .setState(state)
      .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
      .setMessage("From Test")
      .build()
  }

  def createTask(String name) {
    def resources = createResourceList()
    def execInfo = createExecutorInfo()
    def offer = createOffer()
    def taskIdName = createTaskIdName()
    return new Task(resources, execInfo, offer, name, TEST_TYPE, taskIdName)
  }

  def createResourceList() {
    def r = Protos.Resource.newBuilder()
      .setName("name")
      .setType(Protos.Value.Type.SCALAR)
      .setScalar(Protos.Value.Scalar.newBuilder()
      .setValue(1).build())
      .setRole("role")
      .build()

    def resources = new ArrayList<Protos.Resource>()
    resources.add(r)
    return resources
  }

  def createExecutorInfo() {
    return Protos.ExecutorInfo
      .newBuilder()
      .setExecutorId(Protos.ExecutorID.newBuilder().setValue("executor").build())
      .setCommand(
      Protos.CommandInfo
        .newBuilder()
        .addAllUris(
        Arrays.asList(
          Protos.CommandInfo.URI
            .newBuilder()
            .setValue("http://test_url/")
            .build())))
      .build()
  }

  def createOffer() {
    return Protos.Offer.newBuilder()
      .setId(createTestOfferId(1))
      .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework").build())
      .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
      .setHostname(TEST_HOST)
      .build()
  }

  def createTestOfferId(int instanceNumber) {
    return Protos.OfferID.newBuilder().setValue("offer" + instanceNumber).build()
  }

  def createTaskIdName() {
    return "taskIdName_" + new BigInteger(130, random).toString(32)
  }

  def createFrameworkId() {
    return Protos.FrameworkID.newBuilder().setValue(testIdName).build()
  }

}
