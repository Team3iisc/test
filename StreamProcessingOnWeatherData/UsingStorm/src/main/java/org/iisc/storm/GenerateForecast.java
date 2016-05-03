package org.iisc.storm;

import java.util.LinkedList;

import org.rosuda.JRI.REXP;
import org.rosuda.JRI.Rengine;

public class GenerateForecast extends Thread{
	private Rengine rengine;
	LinkedList<Double> buffer;
	int windSize;
	int numRetain;
	String script;
	private int count_train;
	public GenerateForecast(LinkedList<Double> buffer,int windSize,int numRetain,String script,Rengine rengine){
		this.buffer=buffer;
		this.windSize=windSize;
		this.numRetain=numRetain;
		this.script=script;
		this.rengine=rengine;
		count_train=-1;
	}
	public void run() {
		count_train++;
		rengine.eval("wind_size="+windSize);
		rengine.eval("num_retrain="+numRetain);
		rengine.eval("end1=end1+"+(double)(windSize*numRetain));
		rengine.eval("source('"+script+"')");
		REXP result = rengine.eval("fc");
		double[] forecastRes = result.asDoubleArray();
		for(int i=count_train*(windSize*numRetain);i<(count_train+1)*(windSize*numRetain);i++){
			buffer.addLast(forecastRes[i]);
		}
	}
}
