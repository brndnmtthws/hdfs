package org.apache.mesos.hdfs.state;

import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.mesos.hdfs.SchedulerModuleTest;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.scheduler.Reconciler;
import org.apache.mesos.hdfs.util.HDFSConstants;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StateMachineTest {
  private final Injector injector = Guice.createInjector(new SchedulerModuleTest());
  private final HdfsFrameworkConfig config = injector.getInstance(HdfsFrameworkConfig.class);

  private final int TARGET_JOURNAL_COUNT = config.getJournalNodeCount();
  private final int TARGET_NAME_COUNT = HDFSConstants.TOTAL_NAME_NODES;
  private final Reconciler completeReconciler = createMockReconciler(true);
  private final Reconciler incompleteReconciler = createMockReconciler(false);

  @Test
  public void testInitialCorrectPhase() {
    StateMachine sm = createStateMachine(incompleteReconciler);
    assertEquals(AcquisitionPhase.RECONCILING_TASKS, sm.getCurrentPhase());
  }

  @Test
  public void testStayInReconciliationIfIncomplete() {
    StateMachine sm = createStateMachine(incompleteReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.RECONCILING_TASKS, sm.getCurrentPhase());
  }

  @Test
  public void testTransitionFromReconcilingToJournal() {
    StateMachine sm = createStateMachine(completeReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.JOURNAL_NODES, sm.getCurrentPhase());
  }

  @Test
  public void testStayInJournalIfTooFew()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = createMockState(0, 0, false);
    StateMachine sm = createStateMachine(state, completeReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.JOURNAL_NODES, sm.getCurrentPhase());

    setMockState(state, TARGET_JOURNAL_COUNT - 1, 0, false);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.JOURNAL_NODES, sm.getCurrentPhase());
  }

  @Test
  public void testTransitionFromJournalToName()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = createMockState(0, 0, false);
    StateMachine sm = createStateMachine(state, completeReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.JOURNAL_NODES, sm.getCurrentPhase());

    setMockState(state, TARGET_JOURNAL_COUNT, 0, false);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.NAME_NODES, sm.getCurrentPhase());
  }

  @Test
  public void testStayInNameIfTooFew()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = createMockState(TARGET_JOURNAL_COUNT, TARGET_NAME_COUNT - 1, false);
    StateMachine sm = createStateMachine(state, completeReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.NAME_NODES, sm.getCurrentPhase());
  }

  @Test
  public void testStayInNameIfNotInitialized()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = createMockState(TARGET_JOURNAL_COUNT, TARGET_NAME_COUNT, false);
    StateMachine sm = createStateMachine(state, completeReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.NAME_NODES, sm.getCurrentPhase());
  }

  @Test
  public void testTransitionToData()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = createMockState(TARGET_JOURNAL_COUNT, TARGET_NAME_COUNT, true);
    StateMachine sm = createStateMachine(state, completeReconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.DATA_NODES, sm.getCurrentPhase());
  }

  @Test
  public void testTransitionFromDataToReconciling()
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = createMockState(TARGET_JOURNAL_COUNT, TARGET_NAME_COUNT, true);
    Reconciler reconciler = createMockReconciler(true);
    StateMachine sm = createStateMachine(state, reconciler);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.DATA_NODES, sm.getCurrentPhase());

    setMockReconciler(reconciler, false);
    sm.correctPhase();
    assertEquals(AcquisitionPhase.RECONCILING_TASKS, sm.getCurrentPhase());
  }

  private StateMachine createStateMachine(Reconciler reconciler) {
    return createStateMachine(
      injector.getInstance(HdfsState.class),
      reconciler);
  }

  private StateMachine createStateMachine(HdfsState state, Reconciler reconciler) {
    return new StateMachine(state, config, reconciler);
  }

  private HdfsState createMockState(int journalCount, int nameCount, boolean nameInitialized)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    HdfsState state = mock(HdfsState.class);
    return setMockState(state, journalCount, nameCount, nameInitialized);
  }

  private HdfsState setMockState(
    HdfsState state,
    int journalCount,
    int nameCount,
    boolean nameInitialized)
    throws ClassNotFoundException, InterruptedException, ExecutionException, IOException {
    when(state.getJournalCount()).thenReturn(journalCount);
    when(state.getNameCount()).thenReturn(nameCount);
    when(state.nameNodesInitialized()).thenReturn(nameInitialized);
    return state;
  }

  private Reconciler createMockReconciler(boolean complete) {
    Reconciler reconciler = mock(Reconciler.class);
    return setMockReconciler(reconciler, complete);
  }

  private Reconciler setMockReconciler(Reconciler reconciler, boolean complete) {
    when(reconciler.complete()).thenReturn(complete);
    return reconciler;
  }
}
