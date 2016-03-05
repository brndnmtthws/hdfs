package org.apache.mesos.hdfs.state;

import com.google.inject.Inject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.mesos.SchedulerDriver;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.scheduler.Reconciler;
import org.apache.mesos.process.FailureUtils;
import org.apache.mesos.hdfs.util.HDFSConstants;

/**
 * The Scheduler state machine.
 */
public class StateMachine {
  private final HdfsState state;
  private final HdfsFrameworkConfig config;
  private final Log log = LogFactory.getLog(StateMachine.class);
  private final Reconciler reconciler;
  private AcquisitionPhase currPhase;

  @Inject
  public StateMachine(
    HdfsState state,
    HdfsFrameworkConfig config,
    Reconciler reconciler) {
    this.state = state;
    this.config = config;
    this.currPhase = AcquisitionPhase.RECONCILING_TASKS;
    this.reconciler = reconciler;
  }

  public Reconciler getReconciler() {
    return reconciler;
  }

  public AcquisitionPhase getCurrentPhase() {
    return currPhase;
  }

  public AcquisitionPhase correctPhase() {
    int currJournalCount = 0;
    int currNameCount = 0;

    try {
      currJournalCount = state.getJournalCount();
      currNameCount = state.getNameCount();
    } catch (Exception ex) {
      // We will not change phase here if we cannot determine our state
      log.error("Failed to fetch node counts with exception: " + ex);
      return currPhase;
    }

    int targetJournalCount = config.getJournalNodeCount();
    int targetNameCount = HDFSConstants.TOTAL_NAME_NODES;

    log.info(String.format("Correcting phase with journal counts: %s/%s and name counts: %s/%s",
      currJournalCount,
      targetJournalCount,
      currNameCount,
      targetNameCount));

    if (!reconciler.complete()) {
      transitionTo(AcquisitionPhase.RECONCILING_TASKS);
    } else if (currJournalCount < targetJournalCount) {
      transitionTo(AcquisitionPhase.JOURNAL_NODES);
    } else if (currNameCount < targetNameCount || !state.nameNodesInitialized()) {
      transitionTo(AcquisitionPhase.NAME_NODES);
    } else {
      transitionTo(AcquisitionPhase.DATA_NODES);
    }

    log.info("Current phase is now: " + currPhase);
    return currPhase;
  }

  private void transitionTo(AcquisitionPhase nextPhase) {
    if (currPhase.equals(nextPhase)) {
      log.info(String.format("Acquisition phase is already '%s'", currPhase));
    } else {
      log.info(String.format("Transitioning from acquisition phase '%s' to '%s'", currPhase, nextPhase));
      currPhase = nextPhase;
    }
  }

  public void reconcile(SchedulerDriver driver) {
    try {
      transitionTo(AcquisitionPhase.RECONCILING_TASKS);
      reconciler.reconcile(driver);
    } catch (Exception ex) {
      FailureUtils.exit("Failed to conduct Reconciliation with exception: " + ex, HDFSConstants.RECONCILE_EXIT_CODE);
    }
  }
}
