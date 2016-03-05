package org.apache.mesos.hdfs.scheduler;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.SchedulerModuleTest;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.StateMachine;
import org.apache.mesos.protobuf.AttributeUtil;
import org.apache.mesos.protobuf.OfferBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collection;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class SchedulerConstraintsTest {
  private final Injector injector = Guice.createInjector(new SchedulerModuleTest());
  private Configuration config = new Configuration();
  private HdfsFrameworkConfig hdfsConfig = new HdfsFrameworkConfig(config);
  private HdfsState state = injector.getInstance(HdfsState.class);
  private StateMachine stateMachine = createMockStateMachine(AcquisitionPhase.DATA_NODES);

  @Mock
  SchedulerDriver driver;

  @Captor
  ArgumentCaptor<Collection<TaskInfo>> taskInfosCapture;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void acceptOffersWithConstraintMatch() {
    config.set("mesos.hdfs.constraints", "zone:east");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createTextAttribute("zone", "east")).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void declineOffersWithNoConstraintMatch() {
    config.set("mesos.hdfs.constraints", "zone:east");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createTextAttribute("zone", "west")).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).declineOffer(offer.getId());
  }

  @Test
  public void acceptOffersWithConstraintMatchSet() {
    config.set("mesos.hdfs.constraints", "zone:east");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createTextAttributeSet("zone", "east")).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void acceptOffersWithConstraintMatchScalar() {
    config.set("mesos.hdfs.constraints", "CPU:3");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createScalarAttribute("CPU", 3.5))
      .build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void acceptOffersWithConstraintMatchMultiple() {
    config.set("mesos.hdfs.constraints", "CPU:2;ZONE:west");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createTextAttributeSet("ZONE", "west,east,north"))
      .addAttribute(AttributeUtil.createTextAttribute("TYPE", "hi-end"))
      .addAttribute(AttributeUtil.createScalarAttribute("CPU", 3.5))
      .build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void declineOffersWithNoConstraintMatchMultiple() {
    config.set("mesos.hdfs.constraints", "TYPE:low-end;ZONE:north");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createTextAttributeSet("ZONE", "west,east,north"))
      .addAttribute(AttributeUtil.createTextAttribute("TYPE", "hi-end"))
      .addAttribute(AttributeUtil.createScalarAttribute("CPU", 3.5))
      .build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).declineOffer(offer.getId());
  }

  private HdfsScheduler createDefaultScheduler() {
    Reconciler reconciler = mock(Reconciler.class);
    when(stateMachine.getReconciler()).thenReturn(reconciler);
    return new HdfsScheduler(hdfsConfig, state, stateMachine);
  }

  @Test
  public void acceptOffersWithRangeConstraintSpecified() {
    config.set("mesos.hdfs.constraints", "DISKSIZE:500");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createRangeAttribute("DISKSIZE", 100, 1000))
      .build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void acceptOffersWithNoConstraintSpecified() {
    config.set("mesos.hdfs.constraints", "");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = createTestOfferBuilderWithResources(4, 5, 64 * 1024)
      .addAttribute(AttributeUtil.createTextAttribute("zone", "east")).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  private StateMachine createMockStateMachine(AcquisitionPhase phase) {
    StateMachine stateMachine = mock(StateMachine.class);
    when(stateMachine.getCurrentPhase()).thenReturn(phase);
    return stateMachine;
  }

  private OfferBuilder createTestOfferBuilderWithResources(
    int instanceNumber,
    double cpus,
    int mem) {

    ResourceBuilder resourceBuilder = new ResourceBuilder("*");
    return new OfferBuilder("offer" + instanceNumber, "framework1", "slave" + instanceNumber, "host" + instanceNumber)
      .addResource(resourceBuilder.createCpuResource(cpus))
      .addResource(resourceBuilder.createMemResource(mem));
  }
}
