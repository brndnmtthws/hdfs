package org.apache.mesos.hdfs.state;

import org.apache.commons.lang.time.DateUtils;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Date;

/**
 *
 */
public class DeadNodeTracker {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private Timestamp deadJournalNodeTimeStamp = null;
  private Timestamp deadNameNodeTimeStamp = null;
  private Timestamp deadDataNodeTimeStamp = null;

  // todo:  (kgs) see if we can remove this dependency
  private IPersistentStateStore persistenceStore;
  private HdfsFrameworkConfig hdfsFrameworkConfig;


  public DeadNodeTracker(IPersistentStateStore persistenceStore, HdfsFrameworkConfig hdfsFrameworkConfig) {
    this.persistenceStore = persistenceStore;
    this.hdfsFrameworkConfig = hdfsFrameworkConfig;
  }


  public void resetDeadNodeTimeStamps() {
    Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());

    if (persistenceStore.getDeadJournalNodes().size() > 0) {
      deadJournalNodeTimeStamp = new Timestamp(date.getTime());
    }

    if (persistenceStore.getDeadNameNodes().size() > 0) {
      deadNameNodeTimeStamp = new Timestamp(date.getTime());
    }

    if (persistenceStore.getDeadDataNodes().size() > 0) {
      deadDataNodeTimeStamp = new Timestamp(date.getTime());
    }
  }

  public void resetJournalNodeTimeStamp() {
    Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
    deadJournalNodeTimeStamp = new Timestamp(date.getTime());
  }

  public void resetNameNodeTimeStamp() {
    Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
    deadNameNodeTimeStamp = new Timestamp(date.getTime());
  }

  public void resetDataNodeTimeStamp() {
    Date date = DateUtils.addSeconds(new Date(), hdfsFrameworkConfig.getDeadNodeTimeout());
    deadDataNodeTimeStamp = new Timestamp(date.getTime());
  }

  public boolean journalNodeTimerExpired() {
    return deadJournalNodeTimeStamp != null && deadJournalNodeTimeStamp.before(new Date());
  }

  public boolean nameNodeTimerExpired() {
    return deadNameNodeTimeStamp != null && deadNameNodeTimeStamp.before(new Date());
  }

  public boolean dataNodeTimerExpired() {
    return deadDataNodeTimeStamp != null && deadDataNodeTimeStamp.before(new Date());
  }
}
