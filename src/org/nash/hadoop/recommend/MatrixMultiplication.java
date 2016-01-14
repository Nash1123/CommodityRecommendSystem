package org.nash.hadoop.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.nash.hadoop.hdfs.HdfsDAO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MatrixMultiplication {
	
	public static class MatrixMultiplicationMapper extends Mapper<LongWritable, Text, Text, Text> {                
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] flag = values.toString().split(",");
            if (flag.length < 3) {		// CoOcurrenceMatrix
            	String[] tokens = MainRun.DELIMITER.split(values.toString());
            	String[] v1 = tokens[0].split(":");
                String itemID1 = v1[0];
                String itemID2 = v1[1];
                String count = tokens[1];
                Text k = new Text(itemID2);
                Text v = new Text("A:" + itemID1 + "," + count);
                context.write(k, v);             
            } else {		// UserRatingMatrix
                String[] tokens = values.toString().split(",");
                String userID = tokens[0];
                String itemID2 = tokens[1];
                String rate = tokens[2];               
                Text k = new Text(itemID2);
                Text v = new Text("B:" + userID + "," + rate);                
                context.write(k, v);   
            }
        }
    }

	public static class MatrixMultiplicationReducer extends Reducer<Text, Text, Text, Text> {		
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {    
            Map<String, String> mapItemCount = new HashMap<String, String>();
            Map<String, String> mapUserRate = new HashMap<String, String>();            
            for (Text line : values){	
                String val = line.toString();                
                if (val.startsWith("A:")) {
                    String[] itemCount = val.substring(2).split(",");
                    mapItemCount.put(itemCount[0], itemCount[1]);
                } else if (val.startsWith("B:")) {
                    String[] userRate = val.substring(2).split(",");
                    mapUserRate.put(userRate[0], userRate[1]);
                }
            }
            double result = 0;
            Iterator itKeyItem = mapItemCount.keySet().iterator();
            while (itKeyItem.hasNext()) {            	
                String item = (String) itKeyItem.next();                
                int count = Integer.parseInt(mapItemCount.get(item));                
                Iterator itKeyUser = mapUserRate.keySet().iterator();                
                while (itKeyUser.hasNext()) {                	
                    String user = (String) itKeyUser.next();                    
                    double rate = Double.parseDouble(mapUserRate.get(user));
                    result = count * rate;                    
                    Text k = new Text(user + ":" + item);                    
                    Text v = new Text("" + result);                    
                    context.write(k, v);
                }
            }
        }			
    }

    public static void run(Map<String, String> pathMap) throws IOException, InterruptedException, ClassNotFoundException {
    	JobConf conf = MainRun.config();
    	
		String input1 = pathMap.get("CoOcurrenceMatrixOutput") + "/part-00000";
		String input2 = pathMap.get("GroupUserInput") + "/OriData.csv";
		String output = pathMap.get("MatrixMultiplicationOutput");

		HdfsDAO hdfs = new HdfsDAO(MainRun.HDFS, conf);		
		hdfs.rmr(output);

        Job job = new Job(conf);
      		            
        job.setJarByClass(MatrixMultiplication.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MatrixMultiplicationMapper.class);
        job.setReducerClass(MatrixMultiplicationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
