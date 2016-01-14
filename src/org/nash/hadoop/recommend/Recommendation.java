package org.nash.hadoop.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.nash.hadoop.hdfs.HdfsDAO;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Recommendation {
	
	public static class RecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {                
		@Override
		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
			String[] flag = values.toString().split(",");
            if (flag.length < 3) {		// Result Matrix		EX: 1:101	44.0
            	String[] tokens = MainRun.DELIMITER.split(values.toString());
            	String[] v1 = tokens[0].split(":");
                String userID = v1[0];
                String itemID = v1[1];
                String rate = tokens[1];
                Text k = new Text(userID);
                Text v = new Text("B:" + itemID + "," + rate);
                context.write(k, v);             
            } else {		// Original Matrix		EX: 1,101,5.0
                String userID = flag[0];
                String itemID = flag[1];
                String rate = flag[2];               
                Text k = new Text(userID);
                Text v = new Text("A:" + itemID + "," + rate);                
                context.write(k, v);   
            }
        }
    }

	public static class RecommendationReducer extends Reducer<Text, Text, Text, Text> {		
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Map<String, Double> mapA = new HashMap<String, Double>();
            Map<String, Double> mapB = new HashMap<String, Double>();            
            for (Text line : values){	
                String val = line.toString();                
                if (val.startsWith("A:")) {		// EX: A:101,5.0
                    String[] itemRate = val.substring(2).split(",");
                    mapA.put(itemRate[0], Double.parseDouble(itemRate[1]));
                } else if (val.startsWith("B:")) {		// EX: B:101,44.0
                    String[] itemRate = val.substring(2).split(",");
                    mapB.put(itemRate[0], Double.parseDouble(itemRate[1]));
                }
            }
            
            double tryVal = -1;
            double buyAgainVal = -1;
            String tryID = "";
            String buyAgainID = "";
            Random rand = new Random();
            
            for (String keyB: mapB.keySet()) {
            	if (mapA.containsKey(keyB)) {
            		if (mapA.get(keyB) > buyAgainVal) {
            			buyAgainID = keyB;
            			buyAgainVal = mapA.get(keyB); 
            		}else if (mapA.get(keyB) == buyAgainVal) {
            			if (mapB.get(keyB) > mapB.get(buyAgainID)) {
            				buyAgainID = keyB;
                			buyAgainVal = mapA.get(keyB);
            			}else if (mapB.get(keyB) == mapB.get(buyAgainID)) {
            				if (rand.nextInt(2) == 0) {
            					buyAgainID = keyB;
                    			buyAgainVal = mapA.get(keyB);
            				}
            			}
            		}
            	}else {
            		if (mapB.get(keyB) > tryVal) {
            			tryID = keyB;
            			tryVal = mapB.get(keyB);
            		}else if (mapB.get(keyB) == tryVal) {
            			if (rand.nextInt(2) == 0) {
            				tryID = keyB;
                			tryVal = mapB.get(keyB);
            			}
            		}
            	}
            }
            context.write(key, new Text("T:" + tryID + ", BA:" + buyAgainID));
        }			
    }

    public static void run(Map<String, String> pathMap) throws IOException, InterruptedException, ClassNotFoundException {
    	JobConf conf = MainRun.config();    	
		String input1 = pathMap.get("MatrixSummationOutput") + "/part-00000";
		String input2 = pathMap.get("GroupUserInput") + "/OriData.csv";
		String output = pathMap.get("RecommendationOutput");
		
		HdfsDAO hdfs = new HdfsDAO(MainRun.HDFS, conf);		
		hdfs.rmr(output);
		
        Job job = new Job(conf);
      		            
        job.setJarByClass(Recommendation.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(RecommendationMapper.class);
        job.setReducerClass(RecommendationReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(input1), new Path(input2));
        FileOutputFormat.setOutputPath(job, new Path(output));

        job.waitForCompletion(true);
    }
}
