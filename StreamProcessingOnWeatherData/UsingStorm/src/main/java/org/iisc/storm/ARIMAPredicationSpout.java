package org.iisc.storm;

import java.awt.List;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.Map;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class ARIMAPredicationSpout extends BaseRichSpout{
	private SpoutOutputCollector _collector;
	private Rengine rengine;
	private String[] commands;
	private int i;
	private double end;
	private double start;
	LinkedList<Double> buffer=new LinkedList<Double>();
	private static long counter=0;
	private String testScript;
	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		_collector = collector;
		int trainSize=Integer.parseInt(conf.get("train-size").toString());
		String scriptFile=conf.get("script-path").toString();
		String datasetPath=conf.get("dataset-path").toString();
		testScript=conf.get("test-script").toString();
		rengine = new Rengine(new String[] {"--no-save"}, false, null);

		rengine.eval("filepath=\""+datasetPath+"\"");
		end=(double)trainSize+0.9;
		start=(double)trainSize+1.0;
		rengine.eval("start1="+start);
		rengine.eval("end1="+end);
		rengine.eval("source('"+scriptFile+"')");

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		if( buffer.size()<10){
			Thread forecast=new GenerateForecast(buffer,10,20,testScript,rengine);
			forecast.start();
		}
		if(buffer.size()>5){
			double element=buffer.poll();
			//System.out.println("*******Value:   "+element+"******");
			_collector.emit(new Values(element));
		}
		try {
			Thread.sleep(100);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("forecast"));
	}
}
