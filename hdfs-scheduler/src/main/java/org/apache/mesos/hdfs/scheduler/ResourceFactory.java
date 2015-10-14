package org.apache.mesos.hdfs.scheduler;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value;
import org.apache.mesos.hdfs.config.HdfsFrameworkConfig;
import org.apache.mesos.hdfs.config.NodeConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Factory for generating Resources to launch HDFS Nodes.
 */
public class ResourceFactory {
  private HdfsFrameworkConfig config;

  public ResourceFactory(HdfsFrameworkConfig config) {
    this.config = config;
  }

  public Resource createCpuResource(double value) {
    return createScalarResource("cpus", value);
  }

  public Resource createMemResource(double value) {
    return createScalarResource("mem", value);
  }

  public Resource createPortResource(long begin, long end) {
    return createRangeResource("ports", begin, end);
  }

  public List<Resource> getTaskResources(String taskType) {
    NodeConfig nodeConfig = config.getNodeConfig(taskType);
    double cpu = nodeConfig.getCpus();
    double mem = nodeConfig.getMaxHeap() * config.getJvmOverhead();

    List<Resource> resources = new ArrayList<Resource>();
    resources.add(createCpuResource(cpu));
    resources.add(createMemResource(mem));

    return resources;
  }

  private Resource createScalarResource(String name, double value) {
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder()
        .setValue(value).build())
      .setRole(config.getHdfsRole())
      .build();
  }

  private Resource createRangeResource(String name, long begin, long end) {
    Value.Range range = Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addRange(range))
      .build();
  }
}
