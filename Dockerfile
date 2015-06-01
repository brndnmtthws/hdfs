FROM mesosphere/mesos:0.22.1-1.0.ubuntu1404

RUN apt-get update && apt-get -y install gettext

ENV HDFS_MESOS_VERSION 0.1.1

COPY hdfs-mesos-$HDFS_MESOS_VERSION.tgz /hdfs/
RUN cd /hdfs && tar xfz hdfs-mesos-$HDFS_MESOS_VERSION.tgz

ENV MESOS_NATIVE_JAVA_LIBRARY /usr/local/lib/libmesos.so
ENV MESOS_NATIVE_LIBRARY /usr/local/lib/libmesos.so

ADD conf/mesos-site.xml /tmp/mesos-site.xml

WORKDIR /hdfs/hdfs-mesos-$HDFS_MESOS_VERSION

CMD /hdfs/hdfs-mesos-$HDFS_MESOS_VERSION/bin/hdfs-mesos
