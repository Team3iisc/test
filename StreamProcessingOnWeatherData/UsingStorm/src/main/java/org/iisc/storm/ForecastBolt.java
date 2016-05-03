package org.iisc.storm;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ForecastBolt implements IRichBolt{
	private OutputCollector collector;
	private BufferedWriter bufferedWriter;
	private static final Logger LOG = LoggerFactory.getLogger(DataCounterBolt.class);
	private static long counter=0;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
		// TODO Auto-generated method stub
		this.collector = collector;
		File file = new File("/home/rajrup/forecast.dat");
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
		// TODO Auto-generated method stub
		counter++;
		Double forecastTemperature = tuple.getDoubleByField("forecast");
		System.out.println("Forecast Value: "+forecastTemperature);
		String writeStr=counter+" "+forecastTemperature;
		try {
			bufferedWriter.write(writeStr);
			bufferedWriter.newLine();
			bufferedWriter.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
			try {
				bufferedWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
