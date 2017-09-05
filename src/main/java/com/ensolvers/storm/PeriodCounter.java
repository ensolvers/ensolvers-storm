package com.ensolvers.storm;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

public class PeriodCounter implements IRichBolt {

  private static class PeriodCounterDetail {

    private String periodType;
    private String periodValue;
    private String customer;
    private AtomicLong count;
    private Map<String, Object> values;

    public PeriodCounterDetail(String periodType, String periodValue, String customer, Map<String, Object> values) {
      this.periodType = periodType;
      this.periodValue = periodValue;
      this.customer = customer;
      this.count = new AtomicLong();
      this.values = values;
    }

    protected String getPeriodType() {
      return periodType;
    }

    protected String getPeriodValue() {
      return periodValue;
    }

    protected String getCustomer() {
      return customer;
    }

    protected AtomicLong getCount() {
      return count;
    }

    protected Map<String, Object> getValues() {
      return values;
    }
  }

  private static final long serialVersionUID = 1L;
  private OutputCollector collector;
  private Map<String, PeriodCounterDetail> counts;
  private Set<String> dirtyKeys;
  private final String writeTopic;
  private final String producers;
  private KafkaProducer<String, String> producer;
  private final List<String> keyFields;

  public PeriodCounter(String writeTopic, String producers, List<String> keyFields) {
    this.writeTopic = writeTopic;
    this.producers = producers;
    this.counts = new HashMap<String, PeriodCounterDetail>();
    this.dirtyKeys = new ConcurrentSkipListSet<String>();
    this.keyFields = keyFields;
  }

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
    
    Properties config = new Properties();
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.producers);
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    config.put(ProducerConfig.ACKS_CONFIG, "1");
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
    config.put("producer.type", "async");
    config.put("queue.buffering.max.ms", "2000");
    
    this.producer = new KafkaProducer<String, String>(config);
  }

  @Override
  public void execute(Tuple input) {
    if (TupleUtils.isTick(input)) {
      this.flushData();
      return;
    }

    String periodType = input.getStringByField("period_type");
    String periodValue = input.getStringByField("period_value");
    String customer = input.getStringByField("customer");
    String composedKey = periodType + "-" + periodValue + "-" + customer;
    
    Map<String, Object> values = new HashMap<>();

    for (String key : this.keyFields) {
      Object value = input.getValueByField(key);
      composedKey = composedKey + "-" + value.toString();
      values.put(key, value);
    }

    
    
    if (!this.counts.containsKey(composedKey)) {
      synchronized (this.counts) {
        if (!this.counts.containsKey(composedKey)) {
          PeriodCounterDetail detail = new PeriodCounterDetail(periodType, periodValue, customer, values);
          this.counts.put(composedKey, detail);
        }
      }
    }

    this.counts.get(composedKey).getCount().incrementAndGet();
    this.dirtyKeys.add(composedKey);
    this.collector.ack(input);
  }

  private void flushData() {
    for (String composedKey : this.dirtyKeys) {
      PeriodCounterDetail detail = this.counts.get(composedKey);
      Date generated = new Date();

      JSONObject object = new JSONObject();

      try {
        object.put("periodType", detail.getPeriodType());
        object.put("periodValue", detail.getPeriodValue());
        object.put("customer", detail.getCustomer());
        object.put("count", detail.getCount().get());
        object.put("generatedDate", generated.getTime());
        
        for (Map.Entry<String, Object> entry : detail.getValues().entrySet()) {
          object.put(entry.getKey(), entry.getValue());
        }
        
        this.producer.send(new ProducerRecord<String, String>(this.writeTopic, detail.getPeriodValue(), object.toString()));
      } catch (JSONException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    this.dirtyKeys.clear();
  }

  @Override
  public void cleanup() {
    this.producer.close();
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
