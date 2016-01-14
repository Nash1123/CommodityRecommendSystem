package org.nash.hadoop.recommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class CoOcurrenceMatrix {
	public static class CoOcurrenceMatrixMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable(1);

		@Override
		public void map(LongWritable key, Text values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String[] tokens = MainRun.DELIMITER.split(values.toString());
			for (int i = 1; i < tokens.length; i++) {
				String itemID = tokens[i].split(":")[0];
				for (int j = 1; j < tokens.length; j++) {
					String itemID2 = tokens[j].split(":")[0];
					k.set(itemID + ":" + itemID2);
					output.collect(k, v);
				}
			}
		}
	}

	public static class CoOcurrenceMatrixReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		@Override
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			result.set(sum);
			output.collect(key, result);
		}
	}

	public static void run(Map<String, String> pathMap) throws IOException {
		JobConf conf = MainRun.config();

		String input = pathMap.get("GroupUserOutput");
		String output = pathMap.get("CoOcurrenceMatrixOutput");

		HdfsDAO hdfs = new HdfsDAO(MainRun.HDFS, conf);
		hdfs.rmr(output);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		conf.setMapperClass(CoOcurrenceMatrixMapper.class);
		conf.setCombinerClass(CoOcurrenceMatrixReducer.class);
		conf.setReducerClass(CoOcurrenceMatrixReducer.class);

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
