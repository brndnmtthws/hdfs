package org.apache.mesos.hdfs.scheduler

import org.apache.mesos.hdfs.config.HdfsFrameworkConfig
import spock.lang.Specification
import spock.util.environment.RestoreSystemProperties

/**
 *
 */
class HdfsNodeSpec extends Specification {

  HdfsNode hdfsNode
  def config = Mock(HdfsFrameworkConfig)

  def setup() {
    config.getHdfsRole() >> "*"
    hdfsNode = new DataNode(null, config)
  }

  @RestoreSystemProperties
  def "environment with system properties"() {

    when:
    config.getLdLibraryPath() >> "path"
    config.getExecutorHeap() >> 512

    then:
    hdfsNode.getExecutorEnvironment().size() == 2

    when:
    System.properties.put("MESOS_NEW_PROP", "value")

    then:
    hdfsNode.getExecutorEnvironment().size() == 3
  }
}
