package org.iisc.storm;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
//import storm configuration packages
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class DataCounterTopology {
	private static String filename="/home/rajrup/Singapore_15_01.csv";
	private static char separator=',';
	private static boolean includeHeader=true;
	public static void main(String[] args){
		String pathOfCoreSite = (args.length > 0) ? args[0] : "/home/rajrup/Dropbox/SERC/SEMESTER 2/SSDS/Project/UsingStorm/resources/core-site.xml";
		String pathOfHDFSSite = (args.length > 1) ? args[1] : "/home/rajrup/Dropbox/SERC/SEMESTER 2/SSDS/Project/UsingStorm/resources/hdfs-site.xml";
		String inputFileLocation = (args.length > 2) ? args[2] : "/SSDSProject/data/FilePath";
		int filesPerSpout = (args.length > 3) ? Integer.parseInt(args[3]) : 1;
		int spoutsPerBolt = (args.length > 4) ? Integer.parseInt(args[4]) : 1;
		Config config = new Config();
		config.put("core-site", pathOfCoreSite);
		config.put("hdfs-site", pathOfHDFSSite);
		config.setDebug(true);
		config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		config.put("train-size", 80000);
		config.put("script-path", "~/Dropbox/SERC/SEMESTER 2/SSDS/Project/Forecast/R_Dataset/Init_Script.R");
		config.put("dataset-path", "~/Dropbox/SERC/SEMESTER 2/SSDS/Project/Forecast/R_Dataset/Temperature.txt");
		config.put("test-script", "~/Dropbox/SERC/SEMESTER 2/SSDS/Project/Forecast/R_Dataset/Run_Script.R");
		//
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("readSensor", new ReadCSVFileSpout(filename,separator,includeHeader));
		builder.setBolt("filterData", new KalmanFilterBolt()).shuffleGrouping("readSensor");
		builder.setBolt("calculateParam", new CalculateParamBolt()).shuffleGrouping("filterData");
		builder.setBolt("emitData", new DataCounterBolt())
		.shuffleGrouping("calculateParam");
		
		builder.setSpout("forecastStream", new ARIMAPredicationSpout());
		builder.setBolt("emitForecast", new ForecastBolt())
		.shuffleGrouping("forecastStream");

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("SteamSensorData", config, builder.createTopology());
        Utils.sleep(1000000);
        cluster.killTopology("SteamSensorData");
        cluster.shutdown();
	}
}
