package player.data.spark.count;

import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;


public class SimpleApp {

    public static void main(String[] args) {

        String logFile = "/hellodir/count/wordcount.txt";
        // Should be some file on your system
        SparkConf conf = new SparkConf().setAppName("Simple Application");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("hello");
            }
        }).count();

        long numBs = logData.filter(new Function<String, Boolean>() {
            public Boolean call(String s) {
                return s.contains("w");
            }
        }).count();

        System.out.println("Lines with hello: " + numAs + ", lines with w: " + numBs);

        sc.stop();
    }
}
