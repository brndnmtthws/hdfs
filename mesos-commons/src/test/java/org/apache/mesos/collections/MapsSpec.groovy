package org.apache.mesos.collections

import spock.lang.Specification

/**
 *
 */
class MapsSpec extends Specification {

  def "filtering maps"() {
    given:
    def map = ["MESOS_BLAH": "MESOS_BLAH", "MESOS_BLAH2" : "MESOS_BLAH2", "RED_LEADER1" : "Tsui Choi "]

    expect:
    map.size() == 3
    Maps.filterMapKeysThatStartWith(map, "MESOS").size() == 2
    Maps.filterMapKeysThatStartWith(map, "RED").size() == 1
  }
}
