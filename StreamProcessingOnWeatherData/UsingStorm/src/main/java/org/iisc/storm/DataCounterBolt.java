package org.iisc.storm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class DataCounterBolt implements IRichBolt {
	private OutputCollector collector;
	private BufferedWriter bufferedWriter;
	private static final Logger LOG = LoggerFactory.getLogger(DataCounterBolt.class);
	private static long counter=0;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		File file = new File("/home/rajrup/parameter.dat");
		try{
		// if file doesnt exists, then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		FileWriter fw = new FileWriter(file.getAbsoluteFile());
		bufferedWriter = new BufferedWriter(fw);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	@Override
	public void execute(Tuple tuple) {
		counter++;
		// TODO Auto-generated method stub
		String timeElement=tuple.getString(0);
		Double actualTemp=tuple.getDoubleByField("actualTemp");
		Double minTemperature = tuple.getDoubleByField("minTemperature");
		Double maxTemperature=tuple.getDoubleByField("maxTemperature");
		Double meanTemperature =tuple.getDoubleByField("meanTemperature");	
		Double stdDevTemperature =tuple.getDoubleByField("stdDevTemperature");	
		Double firstMoment=tuple.getDoubleByField("firstMoment");	
		Double secondMoment=tuple.getDoubleByField("secondMoment");	
		Double thirdMoment=tuple.getDoubleByField("thirdMoment");
		Double actualSecondMoment=tuple.getDoubleByField("actualSecondMoment");
		Double actualThirdMoment=tuple.getDoubleByField("actualThirdMoment");
		String writeStr=counter+" "+actualTemp+" "+minTemperature+" "+maxTemperature+" "+meanTemperature+" "+stdDevTemperature+" "+firstMoment+" "+secondMoment+" "+thirdMoment+" "+actualSecondMoment+" "+actualThirdMoment;
		try {
			bufferedWriter.write(writeStr);
			bufferedWriter.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("Temperature Parameter "+writeStr);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
