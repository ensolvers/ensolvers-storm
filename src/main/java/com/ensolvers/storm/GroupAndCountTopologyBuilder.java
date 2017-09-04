package com.ensolvers.storm;

import java.util.ArrayList;
import java.util.List;
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

  public static StormTopology build(String customerName, String readTopic, String writeTopic, String zookeeper, String producers, String dateField, List<String> keyFields) {
    TopologyBuilder builder = new TopologyBuilder();    
    
    BrokerHosts zk = new ZkHosts(zookeeper);
    
    //new consumer group every time the topology is deployed
    SpoutConfig spoutConfig = new SpoutConfig(zk, readTopic, "/" + readTopic, UUID.randomUUID().toString()); 
    spoutConfig.startOffsetTime = OffsetRequest.EarliestTime();
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    
    KafkaSpout spout = new KafkaSpout(spoutConfig);
    
    List<String> groupingFields = new ArrayList<>();
    groupingFields.add("period_type");
    groupingFields.add("period_value");
    groupingFields.add("customer");
    groupingFields.addAll(keyFields);
    
    builder.setSpout("messages", spout, 10);   
    builder.setBolt("partitioner", new PeriodPartitioner(dateField, customerName, keyFields), 100).shuffleGrouping("messages");
    builder.setBolt("count", new PeriodCounter(writeTopic, producers), 100).fieldsGrouping("partitioner", new Fields(groupingFields));

    return builder.createTopology();
  }
}
