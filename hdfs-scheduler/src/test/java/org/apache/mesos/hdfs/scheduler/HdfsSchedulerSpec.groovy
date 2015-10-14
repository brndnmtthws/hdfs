package org.apache.mesos.hdfs.scheduler

import com.google.inject.Guice
import org.apache.mesos.Protos
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.hdfs.TestSchedulerModule
import org.apache.mesos.hdfs.state.AcquisitionPhase
import org.apache.mesos.hdfs.state.HdfsState
import org.apache.mesos.hdfs.state.StateMachine
import spock.lang.Shared
import spock.lang.Specification

/**
 *
 */
class HdfsSchedulerSpec extends Specification {

  def injector = Guice.createInjector(new TestSchedulerModule())
  def reconciler = Mock(Reconciler)
  def stateMachine = Mock(StateMachine)
  def driver = Mock(SchedulerDriver)
  def state = injector.getInstance(HdfsState.class)
  def scheduler

  @Shared
  int offerCount = 1

  def setup() {
    stateMachine.reconciler >> reconciler
    scheduler = new HdfsScheduler(null, state, stateMachine)
  }

  def "resource offers - reconciling"() {
    given:
    def constraints = Mock(HdfsMesosConstraints)
    scheduler.hdfsMesosConstraints = constraints

    def offers = []
    offers << createOffer()

    when:
    scheduler.resourceOffers(driver, offers)

    then:
    1 * stateMachine.currentPhase >> AcquisitionPhase.RECONCILING_TASKS


    then:
    1 * stateMachine.correctPhase()
    1 * driver.declineOffer(_)
  }

  def "resource offers - all node types"() {
    given:
    def constraints = Mock(HdfsMesosConstraints)
    def launcher = Mock(NodeLauncher)
    scheduler.hdfsMesosConstraints = constraints
    scheduler.launcher = launcher

    def offers = []
    offers << createOffer()

    when:
    scheduler.resourceOffers(driver, offers)

    then:
    stateMachine.currentPhase >> phase
    0 * stateMachine.correctPhase()
    constraints.constraintsAllow(_) >> true

    then:
    1 * launcher.tryLaunch(_, _, { nodeType.call(it) })

    where:
    phase                          | nodeType
    AcquisitionPhase.JOURNAL_NODES | { node -> node instanceof JournalNode }
    AcquisitionPhase.DATA_NODES    | { node -> node instanceof DataNode }
    AcquisitionPhase.NAME_NODES    | { node -> node instanceof NameNode }

  }

  def "resource offers - exception on launch"() {
    given:
    def constraints = Mock(HdfsMesosConstraints)
    scheduler.hdfsMesosConstraints = constraints
    def launcher = Mock(NodeLauncher)
    scheduler.launcher = launcher

    def offers = []
    offers << createOffer()

    when:
    scheduler.resourceOffers(driver, offers)

    then:
    stateMachine.currentPhase >> AcquisitionPhase.JOURNAL_NODES
    constraints.constraintsAllow(_) >> true

    then:
    1 * launcher.tryLaunch(*_) >> { throw new Exception("houston, we have a problem") }

    then:
    1 * driver.declineOffer(_)
  }

  def "registered"() {
    given:
    def frameworkID = createFrameworkId("frameworkId")

    when:
    scheduler.registered(driver, frameworkID, null)

    then:
    state.frameworkId == frameworkID
    1 * stateMachine.reconcile(driver)
  }

  def "declines offers it doesn't need"() {
    def constraints = Mock(HdfsMesosConstraints)
    scheduler.hdfsMesosConstraints = constraints
    def launcher = Mock(NodeLauncher)
    scheduler.launcher = launcher

    def offers = []
    4.times {
      offers << createOffer()
    }

    when:
    scheduler.resourceOffers(driver, offers)

    then:
    constraints.constraintsAllow(_) >> true
    stateMachine.currentPhase >> AcquisitionPhase.DATA_NODES
    1 * launcher.tryLaunch(*_) >> true
    3 * driver.declineOffer(*_)
  }

  def createFrameworkId(String name) {
    return Protos.FrameworkID.newBuilder().setValue(name).build()
  }

  def createOffer() {
    return Protos.Offer.newBuilder()
      .setId(createTestOfferId(offerCount++))
      .setFrameworkId(Protos.FrameworkID.newBuilder().setValue("framework").build())
      .setSlaveId(Protos.SlaveID.newBuilder().setValue("slave").build())
      .setHostname("host")
      .build()
  }

  def createTestOfferId(int instanceNumber) {
    return Protos.OfferID.newBuilder().setValue("offer" + instanceNumber).build()
  }
}
