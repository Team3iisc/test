package org.iisc.storm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class KalmanFilterBolt implements IRichBolt{
	private OutputCollector collector;
	private static double estimatedError=30;
	private static double processNoise=0.125;
	private static double kalmanGain=0;
	private static double sensorNoise=0.32;
	private static double estdTemperatureVal=0;
	private static final Logger LOG = LoggerFactory.getLogger(KalmanFilterBolt.class);
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		String timestamp=tuple.getString(0);
		String timeElement=timestamp.substring(11, 16);
		String temperature = tuple.getString(4);
		double temperatureVal=0;
		try{
			temperatureVal=Double.parseDouble(temperature);			
			estimatedError+=processNoise;
			kalmanGain=estimatedError/(estimatedError+sensorNoise);
			estdTemperatureVal=estdTemperatureVal+kalmanGain*(temperatureVal-estdTemperatureVal);
			estimatedError=(1-kalmanGain)*estimatedError;
			//System.out.println("Estimated TEmperatrue: "+estdTemperatureVal);
			collector.emit(new Values(timeElement,estdTemperatureVal));
		}catch(Exception e){
			temperatureVal=0;
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("time","temperature"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
