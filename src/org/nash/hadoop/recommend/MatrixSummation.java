package org.nash.hadoop.recommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.nash.hadoop.hdfs.HdfsDAO;

public class MatrixSummation {

	public static class MatrixSummationMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private final static Text k = new Text();
		private final static Text v = new Text();
		@Override
		public void map(LongWritable key, Text values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			
			String[] tokens = MainRun.DELIMITER.split(values.toString());
			k.set(tokens[0]);
			v.set(tokens[1]);
			output.collect(k, v);			
		}
	}

	public static class MatrixSummationReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private final static Text v = new Text();

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			Double sum = 0.0;
			while (values.hasNext()) {
				sum += Double.parseDouble(values.next().toString());
			}
			v.set(sum + "");
			output.collect(key, v);
		}
	}

	public static void run(Map<String, String> pathMap) throws IOException {
		JobConf conf = MainRun.config();

		String input = pathMap.get("MatrixMultiplicationOutput");
		String output = pathMap.get("MatrixSummationOutput");

		HdfsDAO hdfs = new HdfsDAO(MainRun.HDFS, conf);
		hdfs.rmr(output);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(MatrixSummationMapper.class);
		conf.setCombinerClass(MatrixSummationReducer.class);
		conf.setReducerClass(MatrixSummationReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(input));
		FileOutputFormat.setOutputPath(conf, new Path(output));

		RunningJob job = JobClient.runJob(conf);
		while (!job.isComplete()) {
			job.waitForCompletion();
		}
	}
}
