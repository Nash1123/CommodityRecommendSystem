package org.nash.hadoop.recommend;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class GroupUser {

	public static class GroupUserMapper extends MapReduceBase implements Mapper<Object, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		@Override
		public void map(Object key, Text values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			String[] tokens = MainRun.DELIMITER.split(values.toString());
			int userID = Integer.parseInt(tokens[0]);
			String itemID = tokens[1];
			String pref = tokens[2];
			k.set(userID);
			v.set(itemID + ":" + pref);
			output.collect(k, v);
		}
	}

	public static class GroupUserReducer extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text> {
		private final static Text v = new Text();

		@Override
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable, Text> output, Reporter reporter) throws IOException {
			StringBuilder sb = new StringBuilder();
			while (values.hasNext()) {
				sb.append("," + values.next());
			}
			v.set(sb.toString().replaceFirst(",", ""));
			output.collect(key, v);
		}
	}

	public static void run(Map<String, String> pathMap) throws IOException {
		JobConf conf = MainRun.config();

		String input = pathMap.get("GroupUserInput");
		String output = pathMap.get("GroupUserOutput");

		HdfsDAO hdfs = new HdfsDAO(MainRun.HDFS, conf);
		hdfs.rmr(input);
		hdfs.mkdirs(input);
		hdfs.copyFile(pathMap.get("OriData"), input);

		conf.setMapOutputKeyClass(IntWritable.class);
		conf.setMapOutputValueClass(Text.class);

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(GroupUserMapper.class);
		conf.setCombinerClass(GroupUserReducer.class);
		conf.setReducerClass(GroupUserReducer.class);

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