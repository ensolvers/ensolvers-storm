package com.ensolvers.storm;

import java.util.UUID;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import kafka.api.OffsetRequest;

public class GroupAndCountTopologyBuilder {

  public static StormTopology build(String readTopic, String writeTopic, String zookeeper, String producers, String dateField) {
    TopologyBuilder builder = new TopologyBuilder();    
    
    BrokerHosts zk = new ZkHosts(zookeeper);
    
    //new consumer group every time the topology is deployed
    SpoutConfig spoutConfig = new SpoutConfig(zk, readTopic, "/" + readTopic, UUID.randomUUID().toString()); 
    spoutConfig.startOffsetTime = OffsetRequest.EarliestTime();
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    
    KafkaSpout spout = new KafkaSpout(spoutConfig);
    
    builder.setSpout("messages", spout, 10);   
    builder.setBolt("partitioner", new PeriodPartitioner(dateField), 100).shuffleGrouping("messages");
    builder.setBolt("count", new PeriodCounter(writeTopic, producers), 100).fieldsGrouping("partitioner", new Fields("period_type", "period_value", "customer"));

    return builder.createTopology();
  }
}
