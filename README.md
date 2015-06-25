[![Build Status](https://travis-ci.org/mesosphere/hdfs.svg?branch=master)](https://travis-ci.org/mesosphere/hdfs)
HA HDFS on Apache Mesos
======================
Starts 1 active NameNode (with JournalNode and ZKFC), 1 standby NN (+JN,ZKFC), 1 JN, and everything else is DataNodes.

Prerequisites
--------------------------
1. Install `tar`, `unzip`, `wget` in your build host. Set proxy for maven / gradle and wget if needed
2. Install `curl` for all hosts in cluster
3. `$JAVA_HOME` needs to be set on the host running your HDFS scheduler. This can be set through setting the environment variable on the host, `export JAVA_HOME=/path/to/jre`, or specifying the environment variable in Marathon.

**NOTE:** The build process current supports maven and gradle.   The gradle wrapper meta-data is included in the project and is self boot-straping (meaning it isn't a prerequisite install).  Maven as the build system is being deprecated.

Building HDFS-Mesos
--------------------------
1. `./bin/build-hdfs`
2. Run `./bin/build-hdfs nocompile` to skip the `gradlew clean package` step and just re-bundle the binaries.
3. To remove the project build output and downloaded binaries, run `./bin/build-hdfs clean`

**NOTE:** The build process builds the artifacts under the `$PROJ_DIR/build` directory.  A number of zip and tar files are cached under the `cache` directory for faster subsequent builds.   The tarball used for installation is hdfs-mesos-x.x.x.tgz which contains the scheduler and the executor to be distributed.


Installing HDFS-Mesos on your Cluster
--------------------------
1. Upload `hdfs-mesos-*.tgz` to a node in your Mesos cluster (which is built to `$PROJ_DIR/build/hdfs-mesos-x.x.x.tgz`).
2. Extract it with `tar zxvf hdfs-mesos-*.tgz`
3. Customize configuration in `hdfs-mesos-*/etc/hadoop/*-site.xml`
4. Check that `hostname` on that node resolves to a non-localhost IP; update /etc/hosts if necessary

### If you have Hadoop pre-installed in your cluster
If you have Hadoop installed across your cluster, you don't need the Mesos scheduler application to distribute the binaries. You can set the `mesos.hdfs.native-hadoop-binaries` configuration parameter in `mesos-site.xml` if don't want the binaries distributed.

### Mesos-DNS custom configuration
You can see the example configuration in the `example-conf/dcos` directory. Since Mesos-DNS provides native bindings for master detection, we can simply use those names in our mesos and hdfs configurations. The example configuration assumes your Mesos masters and your zookeeper nodes are colocated. If they aren't you'll need to specify your zookeeper nodes separately. Also, note that you are using the example in `example-conf/dcos`, the `mesos.hdfs.native-hadoop-binaries` property needs to be set to `false` if your HDFS binaries are not predistributed.

Starting HDFS-Mesos
--------------------------
1. `cd hdfs-mesos-*`
2. `./bin/hdfs-mesos`
3. Check the Mesos web console to wait until all tasks are RUNNING (monitor status in JN sandboxes)

Using HDFS
--------------------------
See some of the many HDFS tutorials out there for more details and explore the web UI at <br>`http://<ActiveNameNode>:50070`.</br>
Note that you can access commands through `hdfs://<mesos.hdfs.framework.name>/` (default: `hdfs://hdfs/`).
Also here is a quick sanity check:

1. `hadoop fs -ls hdfs://hdfs/` should show nothing for starters
2. `hadoop fs -put /path/to/src_file hdfs://hdfs/`
3. `hadoop fs -ls hdfs://hdfs/` should now list src_file

Resource Reservation Instructions (Optional)
--------------------------

1. In mesos-site.xml, change mesos.hdfs.role to hdfs.
2. On master, add the role for HDFS, by running `echo hdfs > /etc/mesos-master/role` or by setting the `—-role=hdfs`.
3. Then restart the master by running `sudo service mesos-master restart`.
4. On each slave where you want to reserve resources, add specific resource reservations for the HDFS role. Here is one example:
<br>`cpus(*):8;cpus(hdfs):4;mem(*):16384;mem(hdfs):8192 > /etc/mesos-slave/resources`</br> or by setting `—-resources=cpus(*):8;cpus(hdfs):4;mem(*):16384;mem(hdfs):8192`.
5. On each slave with the new settings, stop the mesos slave by running
<br>`sudo service mesos-slave stop`.</br>
6. On each slave with the new settings, remove the old slave state by running
<br>`rm -f /tmp/mesos/meta/slaves/latest`.</br>
Note: This will also remove task state, so you will want to manually kill any running tasks as a precaution.
7. On each slave with the new settings, start the mesos slave by running
<br>`sudo service mesos-slave start`.</br>

Shutdown Instructions (Optional)
--------------------------

1. In Marathon (or your other long-running process monitor) stop the hdfs scheduler application
2. Shutdown the hdfs framework in Mesos: `curl -d "frameworkId=YOUR_FRAMEWORK_ID" -X POST http://YOUR_MESOS_URL:5050/master/shutdown`
3. Access your zookeeper instance: `/PATH/TO/zookeeper/bin/zkCli.sh`
4. Remove hdfs-mesos framework state from zookeeper: `rmr /hdfs-mesos`
5. (Optional) Clear your data directories as specified in your `mesos-site.xml`. This is necessary to relaunch HDFS in the same directory.
