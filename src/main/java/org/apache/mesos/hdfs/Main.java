package org.apache.mesos.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mesos.MesosNativeLibrary;
import org.apache.mesos.state.ZooKeeperState;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Main {

  public static void main(String[] args) throws Exception {
    Properties props = System.getProperties();
    Configuration conf = new Configuration();
    String sitePath = props.getProperty("mesos.site.path", "etc/hadoop");
    int configServerPort = Integer.valueOf(props.getProperty("mesos.hdfs.config.server.port", "8765"));
    conf.addResource(new Path(props.getProperty("mesos.conf.path", sitePath + "/mesos-site.xml")));
    MainConf mainConf = new MainConf(conf, configServerPort);

    MesosNativeLibrary.load(mainConf.getNativeLibrary());
    ZooKeeperState zkState = new ZooKeeperState(mainConf.getStateZkServers(), mainConf.getStateZkTimeout(), TimeUnit.MILLISECONDS,
        "/hdfs-mesos/" + mainConf.getClusterName());
    State state = new State(zkState);
    ClusterState clusterState = new ClusterState(state);

    Thread sched = new Thread(new Scheduler(mainConf, clusterState));
    sched.start();

    new ConfigServer(new SchedulerConf(conf, configServerPort), sitePath, clusterState);
  }

}
