package org.apache.mesos.hdfs.state;

public enum AcquisitionPhase {
  RECONCILING_TASKS, // Waits here for the timeout on (re)registration
  JOURNAL_NODES, // Launches and waits for all journalnodes to start
  NAME_NODE_1, // Launches the fist namenode
  NAME_NODE_2, // Launches the second namenode and formats both namenodes
  DATA_NODES // If everything is healthy the scheduler stays here and tries to launch
             // datanodes on any slave that doesn't have an hdfs task running on it
}
