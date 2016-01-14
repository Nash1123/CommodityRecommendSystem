package org.nash.hadoop.recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.mapred.JobConf;

public class MainRun {

    public static final String HDFS = "";
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");

    public static void main(String[] args) throws Exception {
        Map<String, String> pathMap = new HashMap<String, String>();
        
        // local file path
        pathMap.put("OriData", "/home/hadoop/Documents/OriData.csv");	        
        
        // hdfs file path
        pathMap.put("GroupUserInput", HDFS + "/user/hdfs/recommend");
        pathMap.put("GroupUserOutput", pathMap.get("GroupUserInput") + "/GroupUser");        
        pathMap.put("CoOcurrenceMatrixOutput", HDFS + "/user/hdfs/recommend" + "/CoOcurrenceMatrix");
        pathMap.put("MatrixMultiplicationOutput", HDFS + "/user/hdfs/recommend" + "/MatrixMultiplication");
        pathMap.put("MatrixSummationOutput", HDFS + "/user/hdfs/recommend" + "/MatrixSummation");
        pathMap.put("RecommendationOutput", HDFS + "/user/hdfs/recommend" + "/Recommendation");
        GroupUser.run(pathMap);
        CoOcurrenceMatrix.run(pathMap);
        MatrixMultiplication.run(pathMap);
        MatrixSummation.run(pathMap);
        Recommendation.run(pathMap);
        System.exit(0);
    }

    public static JobConf config() {
        JobConf conf = new JobConf(MainRun.class);
        conf.setJobName("Recommend");
        conf.addResource("core-site.xml");
        conf.addResource("hdfs-site.xml");        
        // conf.addResource("classpath:/hadoop/mapred-site.xml");
        return conf;
    }

}
