FROM mesosphere/mesos:0.21.1-1.1.ubuntu1404

COPY hdfs-mesos-$HDFS_MESOS_VERSION.tgz /hdfs/
RUN cd /hdfs && tar xfz hdfs-mesos-$HDFS_MESOS_VERSION.tgz

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/local/lib/libmesos.so
ENV MESOS_NATIVE_LIBRARY /usr/local/lib/libmesos.so

ENV HDFS_MESOS_VERSION 0.1.0

WORKDIR /hdfs/hdfs-mesos-$HDFS_MESOS_VERSION

CMD ./bin/hdfs-mesos
