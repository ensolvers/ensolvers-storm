package com.ensolvers.storm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;
import org.codehaus.jettison.json.JSONObject;
import org.joda.time.DateTime;


public class PeriodPartitioner implements IRichBolt {

  private static final long serialVersionUID = 1L;

  private OutputCollector collector;
  private final String fieldName;

  public PeriodPartitioner(String fieldName) {
    this.fieldName = fieldName;
  }
  
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
  }

  @Override
  public void execute(Tuple input) {
    try {
      if (TupleUtils.isTick(input)) {
        return;
      }
      
      String jsonAsString = input.getStringByField("str");
      JSONObject json = new JSONObject(jsonAsString);
      if (json.has(this.fieldName)) {
        long dtAsLong = json.getLong(this.fieldName);
        DateTime dt = new DateTime(dtAsLong);
        
        Integer year = dt.getYear();
        Integer month = dt.getMonthOfYear();
        Integer day = dt.getDayOfMonth();
        
        Integer hour = dt.getHourOfDay();
        Integer minute = dt.getMinuteOfHour();
        
        collector.emit(input, new Values("YEAR", year.toString(), "MH"));
        collector.emit(input, new Values("MONTH", year + "-" + month, "MH"));
        collector.emit(input, new Values("DAY", year + "-" + month + "-" + day, "MH"));
        collector.emit(input, new Values("HOUR", year + "-" + month + "-" + day + "-" + hour, "MH"));
        collector.emit(input, new Values("MINUTE", year + "-" + month + "-" + day + "-" + hour + "-" + minute, "MH"));
      }
      
      collector.ack(input);
    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  @Override
  public void cleanup() {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("period_type", "period_value", "customer"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
