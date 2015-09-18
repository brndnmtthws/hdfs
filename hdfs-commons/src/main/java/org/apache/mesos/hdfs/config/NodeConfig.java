package org.apache.mesos.hdfs.config;

/**
 */
public class NodeConfig {
  private String type;
  private int maxHeap;
  private double cpus;
  private int port;

  public double getCpus() {
    return cpus;
  }

  public void setCpus(double cpus) {
    this.cpus = cpus;
  }

  public int getMaxHeap() {
    return maxHeap;
  }

  public void setMaxHeap(int maxHeap) {
    this.maxHeap = maxHeap;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  @Override
  public String toString() {
    return "NodeConfig{" +
      "cpus=" + cpus +
      ", type='" + type + '\'' +
      ", maxHeap=" + maxHeap +
      ", port=" + port +
      '}';
  }
}
