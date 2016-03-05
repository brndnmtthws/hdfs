package org.apache.mesos.hdfs.executor

import org.apache.mesos.hdfs.util.HDFSConstants
import org.apache.mesos.protobuf.TaskInfoBuilder
import spock.lang.Specification

/**
 *
 */
class TaskSpec extends Specification {

  def "task type detection"() {

    expect:
    new Task(new TaskInfoBuilder(taskId, "name", "slaveID").build()).type == type

    where:
    taskId                                | type
    "task.$HDFSConstants.JOURNAL_NODE_ID" | HDFSConstants.JOURNAL_NODE_ID
    "task.$HDFSConstants.NAME_NODE_ID"    | HDFSConstants.NAME_NODE_ID
    "task.$HDFSConstants.DATA_NODE_ID"    | HDFSConstants.DATA_NODE_ID
    "task.$HDFSConstants.ZKFC_NODE_ID"    | HDFSConstants.ZKFC_NODE_ID
    ""                                    | ""
    "junk"                                | ""
  }
}
