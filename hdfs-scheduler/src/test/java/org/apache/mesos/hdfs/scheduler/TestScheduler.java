package org.apache.mesos.hdfs.scheduler;

import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.TestSchedulerModule;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.state.AcquisitionPhase;
import org.apache.mesos.hdfs.state.HdfsState;
import org.apache.mesos.hdfs.state.StateMachine;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.apache.mesos.protobuf.OfferBuilder;
import org.apache.mesos.protobuf.ResourceBuilder;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
public class TestScheduler {
  private final Injector injector = Guice.createInjector(new TestSchedulerModule());
  private HdfsFrameworkConfig config = injector.getInstance(HdfsFrameworkConfig.class);
  private final int TARGET_JOURNAL_COUNT = config.getJournalNodeCount();

  @Mock
  SchedulerDriver driver;

  @Captor
  ArgumentCaptor<Collection<TaskInfo>> taskInfosCapture;

  @Before
  public void init() {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void launchesOnlyNeededNumberOfJournalNodes()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    StateMachine stateMachine = createMockStateMachine(AcquisitionPhase.JOURNAL_NODES);
    HdfsState state = mock(HdfsState.class);
    when(state.getJournalCount()).thenReturn(TARGET_JOURNAL_COUNT);

    HdfsScheduler scheduler = new HdfsScheduler(config, state, stateMachine);
    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));
    verify(driver, never()).launchTasks(anyList(), anyList());
  }

  @Test
  public void launchesNamenodes() {
    StateMachine stateMachine = createMockStateMachine(AcquisitionPhase.NAME_NODES);
    HdfsState state = mock(HdfsState.class);
    when(state.hostOccupied(any(String.class), matches(HDFSConstants.JOURNAL_NODE_ID))).thenReturn(true);

    HdfsScheduler scheduler = new HdfsScheduler(config, state, stateMachine);
    scheduler.resourceOffers(driver, Lists.newArrayList(createTestOffer(0)));

    verify(driver, times(1)).launchTasks(anyList(), taskInfosCapture.capture());
    assertTrue(taskInfosCapture.getValue().size() == 2);

    Iterator<TaskInfo> taskInfoIterator = taskInfosCapture.getValue().iterator();
    String firstTask = taskInfoIterator.next().getName();
    assertTrue(firstTask.contains(HDFSConstants.NAME_NODE_ID)
      || firstTask.contains(HDFSConstants.ZKFC_NODE_ID));

    String secondTask = taskInfoIterator.next().getName();
    assertTrue(secondTask.contains(HDFSConstants.NAME_NODE_ID)
      || secondTask.contains(HDFSConstants.ZKFC_NODE_ID));
  }

  @Test
  public void declinesOffersWithNotEnoughResources() {
    StateMachine stateMachine = createMockStateMachine(AcquisitionPhase.DATA_NODES);
    HdfsState state = injector.getInstance(HdfsState.class);
    HdfsScheduler scheduler = new HdfsScheduler(config, state, stateMachine);

    Offer offer = createTestOfferWithResources(0, 0.1, 64);
    scheduler.resourceOffers(driver, Lists.newArrayList(offer));
    verify(driver, times(1)).declineOffer(offer.getId());
  }

  private StateMachine createMockStateMachine(AcquisitionPhase phase) {
    Reconciler reconciler = mock(Reconciler.class);
    StateMachine stateMachine = mock(StateMachine.class);
    when(stateMachine.getCurrentPhase()).thenReturn(phase);
    when(stateMachine.getReconciler()).thenReturn(reconciler);
    return stateMachine;
  }

  private Offer createTestOfferWithResources(int instanceNumber, double cpus, int mem) {
    ResourceBuilder resourceBuilder = new ResourceBuilder("*");
    return new OfferBuilder("offer" + instanceNumber, "framework1", "slave" + instanceNumber, "host" + instanceNumber)
      .addResource(resourceBuilder.createCpuResource(cpus))
      .addResource(resourceBuilder.createMemResource(mem))
      .build();
  }

  private Offer createTestOffer(int instanceNumber) {
    return new OfferBuilder("offer" + instanceNumber, "framework1", "slave" + instanceNumber, "host" + instanceNumber).build();
  }
}
