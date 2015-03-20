FROM 0.21.1-1.1.ubuntu1404

ENV HDFS_MESOS_VERSION 0.0.2

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/local/lib/libmesos.so
ENV MESOS_NATIVE_LIBRARY /usr/local/lib/libmesos.so

ADD hdfs-mesos-$HDFS_MESOS_VERSION.tgz /

WORKDIR /hdfs-mesos-$HDFS_MESOS_VERSION

CMD ./bin/hdfs-mesos
