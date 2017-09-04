package com.ensolvers.storm;

import java.util.ArrayList;
import java.util.List;
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
  private final String periodFieldName;
  private final String customerName;
  private final List<String> keyFields;

  public PeriodPartitioner(String periodFieldName, String customerName, List<String> keyFields) {
    this.periodFieldName = periodFieldName;
    this.customerName = customerName;
    this.keyFields = keyFields;
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
      if (json.has(this.periodFieldName)) {
        long dtAsLong = json.getLong(this.periodFieldName);
        DateTime dt = new DateTime(dtAsLong);
        
        Integer year = dt.getYear();
        Integer month = dt.getMonthOfYear();
        Integer day = dt.getDayOfMonth();
        
        Integer hour = dt.getHourOfDay();
        Integer minute = dt.getMinuteOfHour();
        
        Values yearValues = new Values("YEAR", year.toString(), this.customerName);
        Values monthValues = new Values("MONTH", year + "-" + month, this.customerName);
        Values dayValues = new Values("DAY", year + "-" + month + "-" + day, this.customerName);
        Values hourValues = new Values("HOUR", year + "-" + month + "-" + day + "-" + hour, this.customerName);
        Values minuteValues = new Values("MINUTE", year + "-" + month + "-" + day + "-" + hour + "-" + minute, this.customerName);

        //extract key values
        List<Object> keyValues = new ArrayList<Object>();
        for (String key : this.keyFields) {
          Object value = json.get(key);
          keyValues.add(value);
        }
        
        yearValues.addAll(keyValues);
        monthValues.addAll(keyValues);
        dayValues.addAll(keyValues);
        hourValues.addAll(keyValues);
        minuteValues.addAll(keyValues);
        
        collector.emit(input, yearValues);
        collector.emit(input, monthValues);
        collector.emit(input, dayValues);
        collector.emit(input, hourValues);
        collector.emit(input, minuteValues);
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
    List<String> allFields = new ArrayList<String>();
    allFields.add("period_type");
    allFields.add("period_value");
    allFields.add("customer");
    allFields.addAll(this.keyFields);
    
    declarer.declare(new Fields(allFields));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
