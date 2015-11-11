package org.apache.mesos.hdfs.config

import org.junit.Rule
import org.junit.rules.TemporaryFolder
import spock.lang.Specification
import spock.util.environment.RestoreSystemProperties

/**
 *
 */
class HdfsFrameworkConfigSpec extends Specification {

  @Rule
  final TemporaryFolder temporaryFolder = new TemporaryFolder()
  File xmlFile

  def setup() {
    temporaryFolder.create()
    xmlFile = file("mesos-site.xml")
    System.setProperty("mesos.conf.path", xmlFile.absolutePath)
  }

  @RestoreSystemProperties
  def "system property override"() {
    given:
    createXML()

    when:
    def config = new HdfsFrameworkConfig()
    def datadir = config.dataDir
    def fwName = config.frameworkName

    then:
    datadir == "/var/lib/hdfs/data"
    fwName == "hdfs"

    when:
    System.setProperty("MESOS_HDFS_DATA_DIR", "spacetime")
    System.setProperty("MESOS_HDFS_FRAMEWORK_NAME", "einstein")
    config = new HdfsFrameworkConfig()
    datadir = config.dataDir
    fwName = config.frameworkName

    then:
    datadir == "spacetime"
    fwName == "einstein"
  }

  def createXML() {
    xmlFile << """
      <configuration>
        <property>
          <name>mesos.hdfs.data.dir</name>
          <description>The primary data directory in HDFS</description>
          <value>/var/lib/hdfs/data</value>
        </property>
        <property>
          <name>mesos.hdfs.framework.name</name>
          <description>Your Mesos framework name and cluster name when accessing files (hdfs://YOUR_NAME)</description>
          <value>hdfs</value>
        </property>
      </configuration>
      """
  }

  File file(String name) {
    def file = new File(temporaryFolder.root, name)
    file.parentFile.mkdirs()
    return file
  }
}
