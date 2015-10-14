package org.apache.mesos.hdfs.scheduler;

import com.google.protobuf.ByteString;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.Resource;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskStatus;

import java.io.IOException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.List;

/**
 * Task class encapsulates TaskInfo and metadata necessary for recording State when appropriate.
 */
public class Task implements Serializable {
  private TaskInfo info;
  private TaskStatus status;
  private Offer offer;
  private String type;
  private String name;

  public Task(
    List<Resource> resources,
    ExecutorInfo execInfo,
    Offer offer,
    String name,
    String type,
    String idName) {

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

    setStatus(null);
    this.offer = offer;
    this.type = type;
    this.name = name;
  }

  public TaskID getId() {
    return getInfo().getTaskId();
  }

  public TaskInfo getInfo() {
    return info;
  }

  public TaskStatus getStatus() {
    return status;
  }

  public Offer getOffer() {
    return offer;
  }

  public String getType() {
    return type;
  }

  public String getName() {
    return name;
  }

  public String getHostname() {
    return offer.getHostname();
  }

  public void setStatus(TaskStatus status) {
    this.status = status;
  }

  private void writeObject(java.io.ObjectOutputStream out) throws IOException {
    out.defaultWriteObject();
  }

  private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
    in.defaultReadObject();
  }

  private static class TaskDeserializationException extends ObjectStreamException {
  }

  private void readObjectNoData() throws ObjectStreamException {
    throw new TaskDeserializationException();
  }
}
