package org.apache.mesos.hdfs.scheduler;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.mesos.Protos.Attribute;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.OfferID;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.SlaveID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.Protos.Value.Range.Builder;
import org.apache.mesos.Protos.Value.Type;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.TestSchedulerModule;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.StateMachine;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class TestSchedulerConstraints {
  private final Injector injector = Guice.createInjector(new TestSchedulerModule());
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

    Offer offer = addAttribute(
      createTestOfferBuilderWithResources(4, 5, 64 * 1024), "zone", "east",
      Value.Type.TEXT).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void declineOffersWithNoConstraintMatch() {
    config.set("mesos.hdfs.constraints", "zone:east");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = addAttribute(
      createTestOfferBuilderWithResources(4, 5, 64 * 1024), "zone", "west",
      Value.Type.TEXT).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).declineOffer(offer.getId());
  }

  @Test
  public void acceptOffersWithConstraintMatchSet() {
    config.set("mesos.hdfs.constraints", "zone:east");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = addAttribute(
      createTestOfferBuilderWithResources(4, 5, 64 * 1024), "zone",
      "west,east", Value.Type.SET).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void acceptOffersWithConstraintMatchScalar() {
    config.set("mesos.hdfs.constraints", "CPU:3");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = addAttribute(
      createTestOfferBuilderWithResources(4, 5, 64 * 1024), "CPU", "3.5",
      Value.Type.SCALAR).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void acceptOffersWithConstraintMatchMultiple() {
    config.set("mesos.hdfs.constraints", "CPU:2;ZONE:west");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer.Builder builder = createTestOfferBuilderWithResources(
      4,
      5,
      64 * 1024);
    builder = addAttribute(builder, "CPU", "3.5", Value.Type.SCALAR);
    builder = addAttribute(builder, "ZONE", "west,east,north", Value.Type.SET);
    builder = addAttribute(builder, "TYPE", "hi-end", Value.Type.TEXT);

    scheduler.resourceOffers(driver, Lists.newArrayList(builder.build()));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void declineOffersWithNoConstraintMatchMultiple() {
    config.set("mesos.hdfs.constraints", "TYPE:low-end;ZONE:north");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer.Builder builder = createTestOfferBuilderWithResources(
      4,
      5,
      64 * 1024);
    builder = addAttribute(builder, "CPU", "3.5", Value.Type.SCALAR);
    builder = addAttribute(builder, "ZONE", "west,east,north", Value.Type.SET);
    builder = addAttribute(builder, "TYPE", "hi-end", Value.Type.TEXT);
    Offer offer = builder.build();

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

    Offer.Builder builder = createTestOfferBuilderWithResources(
      4,
      5,
      64 * 1024);
    builder = addAttribute(builder, "DISKSIZE", "100-1000", Value.Type.RANGES);
    Offer offer = builder.build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  @Test
  public void acceptOffersWithNoConstraintSpecified() {
    config.set("mesos.hdfs.constraints", "");
    HdfsScheduler scheduler = createDefaultScheduler();

    Offer offer = addAttribute(
      createTestOfferBuilderWithResources(4, 5, 64 * 1024),
      "zone",
      "east",
      Value.Type.TEXT).build();

    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
  }

  private StateMachine createMockStateMachine(AcquisitionPhase phase) {
    StateMachine stateMachine = mock(StateMachine.class);
    when(stateMachine.getCurrentPhase()).thenReturn(phase);
    return stateMachine;
  }

  private Offer.Builder createTestOfferBuilderWithResources(
    int instanceNumber,
    double cpus,
    int mem) {
    return Offer
      .newBuilder()
      .setId(createTestOfferId(instanceNumber))
      .setFrameworkId(
        FrameworkID.newBuilder().setValue("framework1").build())
      .setSlaveId(
        SlaveID.newBuilder().setValue("slave" + instanceNumber)
          .build())
      .setHostname("host" + instanceNumber)
      .addAllResources(
        Arrays
          .asList(
            Resource
              .newBuilder()
              .setName("cpus")
              .setType(Value.Type.SCALAR)
              .setScalar(
                Value.Scalar.newBuilder().setValue(cpus)
                  .build()).setRole("*").build(),
            Resource
              .newBuilder()
              .setName("mem")
              .setType(Value.Type.SCALAR)
              .setScalar(
                Value.Scalar.newBuilder().setValue(mem)
                  .build()).setRole("*").build()));
  }

  private Offer.Builder addAttribute(
    Offer.Builder offerBuilder,
    String attributeName,
    String value,
    Type t) {

    Attribute.Builder attributeBuilder = Attribute
      .newBuilder()
      .setType(t)
      .setName(attributeName)
      .setText(
        Value.Text.newBuilder()
          .setValue(t == Value.Type.TEXT ? value : "").build())
      .setScalar(
        Value.Scalar
          .newBuilder()
          .setValue(
            t == Value.Type.SCALAR ? Double.parseDouble(value)
              : 0.0).build())
      .setSet(
        Value.Set
          .newBuilder()
          .addAllItem(
            new ArrayList<String>(Arrays.asList(value.split(","))))
          .build());

    if (t == Value.Type.RANGES) {
      Builder rangeBuilder = Value.Range.newBuilder().setBegin(0)
        .setEnd(0);
      if (!StringUtils.isBlank(value)) {
        String[] rangeValues = value.split("-");
        if (rangeValues.length >= 1 && !StringUtils.isBlank(rangeValues[0])) {
          long startValue = Long.parseLong(rangeValues[0]);
          rangeBuilder = rangeBuilder.setBegin(startValue);
        } else {
          rangeBuilder.clearEnd();
        }
        if (rangeValues.length >= 2 && !StringUtils.isBlank(rangeValues[1])) {
          long endValue = Long.parseLong(rangeValues[1]);
          rangeBuilder = rangeBuilder.setEnd(endValue);
        }
      }
      attributeBuilder.setRanges(Value.Ranges.newBuilder().addRange(
        rangeBuilder.build()));
    }

    return offerBuilder.addAttributes(attributeBuilder.build());
  }

  private OfferID createTestOfferId(int instanceNumber) {
    return OfferID.newBuilder().setValue("offer" + instanceNumber)
      .build();
  }
}
