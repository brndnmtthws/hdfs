package org.apache.mesos.hdfs;

public class ResourceRoles {
  public String cpuRole = null;
  public String memRole = null;
  public String portsRole = null;
  // Need to remember these ports.
  public long portsBegin;
  public long portsEnd;

  public ResourceRoles() {
  }

  public ResourceRoles(ResourceRoles other) {
    this.cpuRole = other.cpuRole;
    this.memRole = other.memRole;
    this.portsRole = other.portsRole;
    this.portsBegin = other.portsBegin;
    this.portsEnd = other.portsEnd;
  }
}
