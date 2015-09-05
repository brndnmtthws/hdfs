package org.apache.mesos.hdfs.scheduler;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

import java.util.List;

/**
 * Task class encapsulates TaskInfo and metadata necessary for recording State when appropriate.
 */
public class Task {
  private TaskInfo info;
  private String hostName;
  private String type;
  private String name;

  public Task(List<Resource> resources, ExecutorInfo execInfo, Offer offer, String name, String type, String idName) {
    TaskID taskId = TaskID.newBuilder()
      .setValue(String.format("task.%s.%s", type, idName))
      .build();

    this.info = TaskInfo.newBuilder()
      .setExecutor(execInfo)
      .setName(name)
      .setTaskId(taskId)
      .setSlaveId(offer.getSlaveId())
      .addAllResources(resources)
      .setData(ByteString.copyFromUtf8(String.format("bin/hdfs-mesos-%s", type)))
      .build();

    this.hostName = offer.getHostname();
    this.type = type;
    this.name = name;
  }

  public TaskID getId() {
    return getInfo().getTaskId();
  }

  public TaskInfo getInfo() {
    return info;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getHostname() {
    return hostName;
  }
}
