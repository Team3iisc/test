package org.iisc.storm;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.Iterator;
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

public class CalculateParamBolt implements IRichBolt{
	private OutputCollector collector;
	private static double minTemperature=Double.MAX_VALUE;
	private static double maxTemperature=Double.MIN_VALUE;
	private static double meanTemperature=0;
	private static double varianceTemperature=0;
	private static double stdDevTemperature=0;
	private static long counter=0;
	private static BigDecimal sumTemperature=BigDecimal.ZERO;
	private static BigDecimal squaredDiffSum=BigDecimal.ZERO;
	private static HashMap<Double,Long> frequencyOfTempElement=new HashMap<Double,Long>();
	private static double firstMoment;
	private static double secondMoment;
	private static double thirdMoment;
	private static double actualSecondMoment;
	private static double actualThirdMoment;
	private static final Logger LOG = LoggerFactory.getLogger(CalculateParamBolt.class);
	private static final int randomInterval=5;
	private static final int maxMapSize=10000;
	private static BigDecimal squaredSum=BigDecimal.ZERO;
	private static BigDecimal cubeSum=BigDecimal.ZERO;
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		this.collector = collector;
	}

	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		counter++;
		String timeElement=tuple.getString(0);
		double temperature = tuple.getDouble(1);
		double temperatureVal=temperature;
		/*try{
			temperatureVal=Double.parseDouble(temperature);
		}catch(Exception e){
			temperatureVal=0;
		}*/
		minTemperature= minTemperature>temperatureVal ? temperatureVal:minTemperature;
		maxTemperature=maxTemperature<temperatureVal ? temperatureVal:maxTemperature;		
		sumTemperature=sumTemperature.add(new BigDecimal(temperatureVal));		
		meanTemperature=sumTemperature.divide(new BigDecimal(counter),4, RoundingMode.HALF_UP).doubleValue();
		squaredDiffSum=squaredDiffSum.add(new BigDecimal(Math.pow((temperatureVal-meanTemperature), 2)));
		varianceTemperature=squaredDiffSum.divide(new BigDecimal(counter),4, RoundingMode.HALF_UP).doubleValue();
		stdDevTemperature=Math.sqrt(varianceTemperature);

		//Calculating Moments

		if(!frequencyOfTempElement.containsKey(temperatureVal)){
			if(counter%randomInterval==0 && frequencyOfTempElement.size()<=maxMapSize){
				frequencyOfTempElement.put(temperatureVal, 1L);
			}
		}else{
			long elementVal=frequencyOfTempElement.get(temperatureVal);
			elementVal++;
			frequencyOfTempElement.put(temperatureVal, elementVal);
		}

		firstMoment=meanTemperature;		
		double[] moments=getThirdMoments(counter);
		secondMoment=moments[0];
		thirdMoment=moments[1];
		
		
		
		//Actual moments
		double squareVal=temperatureVal*temperatureVal;
		double cubeVal=squareVal*temperatureVal;
		squaredSum=squaredSum.add(new BigDecimal(squareVal));
		cubeSum=cubeSum.add(new BigDecimal(cubeVal));
		
		actualSecondMoment=squaredSum.divide(new BigDecimal(counter),4, RoundingMode.HALF_UP).doubleValue();
		actualThirdMoment=cubeSum.divide(new BigDecimal(counter),4, RoundingMode.HALF_UP).doubleValue();
		
		collector.emit(new Values(timeElement,temperatureVal,minTemperature,maxTemperature,meanTemperature,stdDevTemperature,firstMoment,secondMoment,thirdMoment,actualSecondMoment,actualThirdMoment));
		//System.out.println("time: "+timeElement+"min: "+minTemperature+" max: "+maxTemperature+" mean: "+meanTemperature+" std: "+stdDevTemperature+" sumVal: "+sumTemperature+"  sumSqr: "+squaredDiffSum+" firstMoment: "+firstMoment+" secondMoment: "+secondMoment+" thirdMoment: "+thirdMoment);
	}


	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("time","actualTemp","minTemperature","maxTemperature","meanTemperature","stdDevTemperature","firstMoment","secondMoment","thirdMoment","actualSecondMoment","actualThirdMoment"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
	public double[] getThirdMoments(long n){
		double[] moments={0,0};
		long sumOfMagicNumber=0;
		long sumThirdMoment=0;
		Iterator it = frequencyOfTempElement.entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Map.Entry)it.next();
			double elementVal=(double) pair.getKey();
			long countOfElement=(long) pair.getValue();
			//it.remove(); // avoids a ConcurrentModificationException	 
			sumOfMagicNumber+=n*(2*countOfElement-1);
			sumThirdMoment+=n*(3*countOfElement*(countOfElement-1)+1);

		}
		long sizeOfMap=frequencyOfTempElement.size();
		if(sizeOfMap!=0){
			//return sumOfMagicNumber/sizeOfMap;
			moments[0]=sumOfMagicNumber/sizeOfMap;
			moments[1]=sumThirdMoment/sizeOfMap;

		}
		return moments;
	}
}
