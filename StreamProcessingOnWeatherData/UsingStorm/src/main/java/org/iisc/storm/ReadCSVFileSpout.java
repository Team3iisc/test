package org.iisc.storm;

import au.com.bytecode.opencsv.CSVReader;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class ReadCSVFileSpout extends BaseRichSpout{  
	private final String fileName;
	private final char separator;
	private boolean includesHeaderRow;
	private SpoutOutputCollector _collector;
	private CSVReader reader;
	private AtomicLong linesRead;
	private FileSystem fs;

	public ReadCSVFileSpout(String filename, char separator, boolean includesHeaderRow) {
		this.fileName = filename;
		this.separator = separator;
		this.includesHeaderRow = includesHeaderRow;
		linesRead=new AtomicLong(0);
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		Configuration configuration = new Configuration();
		configuration.addResource(new Path(conf.get("core-site").toString()));
		configuration.addResource(new Path(conf.get("hdfs-site").toString()));
		System.out.println(conf.get("core-site"));
		System.out.println(conf.get("hdfs-site"));
		try {
			fs = FileSystem.newInstance(configuration);			
			Path pt = new Path("hdfs://localhost/user/rajrup/input/Singapore_15_01.csv");
			//Path pt = new Path("arbit file");
			//reader = new BufferedReader(new InputStreamReader(fs.open(pt)));
			reader = new CSVReader(new InputStreamReader(fs.open(pt)), separator);
			// read and ignore the header if one exists
			if (includesHeaderRow) reader.readNext();
			//System.out.println(reader.readLine());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void nextTuple() {
		try {
			//System.out.println(reader);
			String[] line = reader.readNext();
			if (line != null) {
				//long id=linesRead.incrementAndGet();
				_collector.emit(new Values(line));
				Thread.sleep(100);
			}
			else{
				//System.out.println("Finished reading file, "+linesRead.get()+" lines read");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void ack(Object id) {
	}

	@Override
	public void fail(Object id) {
		System.err.println("Failed tuple with id "+id);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		try {
			CSVReader reader = new CSVReader(new FileReader(fileName), separator);
			// read csv header to get field info
			String[] fields = reader.readNext();
			if (includesHeaderRow) {
				System.out.println("DECLARING OUTPUT FIELDS");
				for (String a : fields)
					System.out.println(a);

				declarer.declare(new Fields(Arrays.asList(fields)));
			} else {
				// if there are no headers, just use field_index naming convention
				ArrayList<String> f= new ArrayList<String>(fields.length);
				for (int i = 0; i < fields.length; i++) {
					f.add("field_"+i);
				}
				declarer.declare(new Fields(f));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
