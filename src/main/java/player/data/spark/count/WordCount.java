package player.data.spark.count;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Arrays;


public class WordCount {

    private static final Logger LOGGER = LoggerFactory.getLogger(WordCount.class);

    private static void run(String inputFilePath) {

        String master = "local[*]";

        SparkConf conf = new SparkConf()
                .setAppName(WordCount.class.getName())
                .setMaster(master);

        JavaSparkContext context = new JavaSparkContext(conf);

        context.textFile(inputFilePath)
                .flatMap(text -> Arrays.asList(text.split(",")).iterator())
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .foreach(
                        result -> {

                            System.out.println();
                            System.out.println(
                                    String.format("%s,  %d", result._1(), result._2)
                            );

                        }
                );
    }

    public static void main(String[] args) {

        String inputFilePath = "/root/Documents/dataseed/sampledata/wordcount.txt";
//        String inputFilePath = "/hellodir/count/wordcount.txt";

        run(inputFilePath);
    }
}
