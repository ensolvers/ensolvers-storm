# This module provides utility functions for Apache Storm

Period partitioner

Giving a list of messages containing date/time information in a Kafka topic, we would like to count the number of messages for a specific type of period: YEAR, MONTH, DAY, HOUR, MINUTE.

For instance having one message in date 2017-11-27 18:20 and another message in 2017-11-27 18:21 the count will be:

YEAR - 2017 - Count 2
MONTH - 2017-11 - Count 2
DAY - 2017-11-27 - Count 2
HOUR - 2017-11-27 18 - Count 2
MINUTE - 2017-11-27 18:20 - Count 1
MINUTE - 2017-11-27 18:21 - Count 1

To be able to run the out of the box solution you need to run the storm submitter:

/opt/storm/bin/storm jar ./mh-storm.jar com.ensolvers.storm.GroupAndCountTopologyRunner \
<topology-name> \
<zookeeper-urls> \
<producer-url> \
<read-topic> \
<json-field> \
<write-topic> \
<local-remote-cluster>

For example:

/opt/storm/bin/storm jar ./mh-storm.jar com.ensolvers.storm.GroupAndCountTopologyRunner \
email-stats-topology \
zk-1.prod.ensolvers.com:2181,zk-2.prod.ensolvers.com:2181,zk-3.prod.ensolvers.com:2181 \
kafka-1.prod.ensolvers.com:9092 \
email_sent_2 \
sentDateTime \
email_stats_2 \
remote