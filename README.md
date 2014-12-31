[![Build Status](https://travis-ci.org/mesosphere/hdfs.svg?branch=master)](https://travis-ci.org/mesosphere/hdfs)
HA HDFS on Apache Mesos
======================
Starts 1 active NameNode (with JournalNode and ZKFC), 1 standby NN (+JN,ZKFC), 1 JN, and everything else is DataNodes.

Building HDFS-Mesos
--------------------------
1. `./bin/build-hdfs`
2. To remove the project build output and downloaded binaries, run `./bin/build-hdfs clean`

Installing HDFS-Mesos on your Cluster
--------------------------
1. Upload `hdfs-mesos-*.tgz` to a node in your Mesos cluster.
2. Extract it with `tar zxvf hdfs-mesos-*.tgz`
3. Customize configuration in `hdfs-mesos-*/etc/hadoop/*-site.xml`
4. Check that `hostname` on that node resolves to a non-localhost IP; update /etc/hosts if necessary

Starting HDFS-Mesos
--------------------------
1. `cd hdfs-mesos-*`
2. `./bin/hdfs-mesos`
3. Check the Mesos web console to wait until all tasks are RUNNING (monitor status in JN sandboxes)

Using HDFS
--------------------------
See some of the many HDFS tutorials out there for more details, but here's a quick sanity check:
1. `hadoop fs -ls hdfs://<ActiveNameNode>:50071/` should show nothing for starters
2. `hadoop fs -put /path/to/src_file hdfs://<ActiveNameNode>:50071/`
3. `hadoop fs -ls hdfs://<ActiveNameNode>:50071/` should now list src_file
