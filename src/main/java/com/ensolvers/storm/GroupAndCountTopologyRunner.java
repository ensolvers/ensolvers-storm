package com.ensolvers.storm;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

public class GroupAndCountTopologyRunner {

  public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    String topologyName = StringUtils.trim(args[0]);
    String zookeeper = StringUtils.trim(args[1]);
    String producers = StringUtils.trim(args[2]);
    String readTopic = StringUtils.trim(args[3]);
    String dateField = StringUtils.trim(args[4]);
    String writeTopic = StringUtils.trim(args[5]);
    String localParam = StringUtils.trim(args[6]);
    
    boolean local = StringUtils.equalsIgnoreCase(localParam, "local");
    
    new GroupAndCountTopologyRunner(zookeeper, producers, readTopic, writeTopic, dateField)
      .start(topologyName, local);
  }
  
  private StormTopology topology;
  
  public GroupAndCountTopologyRunner(String zookeeper, String producers, String readTopic, String writeTopic, String dateField) {
    this.topology = GroupAndCountTopologyBuilder.build(readTopic, writeTopic, zookeeper, producers, dateField);
}
  
  private void start(String topologyName, boolean local) throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
    Config conf = new Config();
    conf.setNumWorkers(20);
    conf.setMaxSpoutPending(5000);
    conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 15);
    
    if (local) {
      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology(topologyName, conf, this.topology);
      Utils.sleep(1000000);
      cluster.killTopology(topologyName);
      cluster.shutdown();    
    } else {
      StormSubmitter.submitTopology(topologyName, conf, this.topology);
    }
  }
}
