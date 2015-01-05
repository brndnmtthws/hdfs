package org.apache.mesos.hdfs.util;

public class HDFSConstants {
  // messages
  public static final String NAME_NODE_INIT_MESSAGE = "-i";
  public static final String NAME_NODE_BOOTSTRAP_MESSAGE = "-b";

  // node ids
  public static final String NAME_NODE_ID = "namenode";
  public static final String JOURNAL_NODE_ID = "journalnode";
  public static final String DATA_NODE_ID = "datanode";
  public static final String ZKFC_NODE_ID = "zkfc";

  // node task ids
  public static final String JOURNAL_NODE_TASKID = ".journalnode.";
  public static final String NAME_NODE_TASKID = ".namenode.namenode.";
  public static final String ZKFC_NODE_TASKID = ".zkfc.";

  // executor ids
  public static final String NODE_EXECUTOR_ID = "NodeExecutor";
  public static final String NAME_NODE_EXECUTOR_ID = "NameNodeExecutor";

}
