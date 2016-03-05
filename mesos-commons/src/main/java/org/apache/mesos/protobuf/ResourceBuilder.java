package org.apache.mesos.protobuf;

import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.Value;

/**
 * Builder class for working with protobufs.  It includes 2 different approaches;
 * 1) static functions useful for developers that want helpful protobuf functions for Resource.
 * 2) builder class
 * All builder classes provide access to the protobuf builder for capabilities beyond the included
 * helpful functions.
 * <p/>
 * This builds Resource objects and provides some convenience functions for common resources.
 */

public class ResourceBuilder {
  private String role;
  static final String DEFAULT_ROLE = "*";


  public ResourceBuilder(String role) {
    this.role = role;
  }

  public Resource createCpuResource(double value) {
    return cpus(value, role);
  }

  public Resource createMemResource(double value) {
    return mem(value, role);
  }

  public Resource createPortResource(long begin, long end) {
    return ports(begin, end, role);
  }

  public Resource createScalarResource(String name, double value) {
    return ResourceBuilder.createScalarResource(name, value, role);
  }

  public Resource createRangeResource(String name, long begin, long end) {
    return ResourceBuilder.createRangeResource(name, begin, end, role);
  }

  public static Resource createScalarResource(String name, double value, String role) {
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder().setValue(value).build())
      .setRole(role)
      .build();
  }

  public static Resource createRangeResource(String name, long begin, long end, String role) {
    Value.Range range = Value.Range.newBuilder().setBegin(begin).setEnd(end).build();
    return Resource.newBuilder()
      .setName(name)
      .setType(Value.Type.RANGES)
      .setRanges(Value.Ranges.newBuilder().addRange(range))
      .build();
  }

  public static Resource cpus(double value, String role) {
    return createScalarResource("cpus", value, role);
  }

  public static Resource cpus(double value) {
    return cpus(value, DEFAULT_ROLE);
  }

  public static Resource mem(double value, String role) {
    return createScalarResource("mem", value, role);
  }

  public static Resource mem(double value) {
    return mem(value, DEFAULT_ROLE);
  }

  public static Resource ports(long begin, long end, String role) {
    return createRangeResource("ports", begin, end, role);
  }

  public static Resource ports(long begin, long end) {
    return ports(begin, end, DEFAULT_ROLE);
  }
}
