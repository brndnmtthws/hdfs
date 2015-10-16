package org.apache.mesos.hdfs.scheduler

import com.google.inject.Guice
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.hdfs.TestSchedulerModule
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig
import org.apache.mesos.hdfs.state.AcquisitionPhase
import org.apache.mesos.hdfs.state.HdfsState
import org.apache.mesos.hdfs.state.StateMachine
import org.apache.mesos.protobuf.FrameworkInfoUtil
import org.apache.mesos.protobuf.OfferBuilder
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
  def config = Mock(HdfsFrameworkConfig)

  @Shared
  int offerCount = 1

  def setup() {
    stateMachine.reconciler >> reconciler
    config.hdfsRole >> "*"
    scheduler = new HdfsScheduler(config, state, stateMachine)
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
    def frameworkID = FrameworkInfoUtil.createFrameworkId("frameworkId")

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

  def createOffer() {
    return OfferBuilder.createOffer("framework", offerCount++ as String, "slave", "host")
  }

  def createTestOfferId(int instanceNumber) {
    return OfferBuilder.createOfferID("offer" + instanceNumber)
  }
}
