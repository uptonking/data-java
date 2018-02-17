package player.data.hadoop.count;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by yaoo on 2/13/18
 */
public class WordCountMain {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        String pathBase = "/opt/zoo/env/hadoop-2.7.2/etc/hadoop/";

        String[] hadoopConfArr = {
                pathBase + "core-site.xml",
                pathBase + "hdfs-site.xml",
                pathBase + "mapred-site.xml",
                pathBase + "yarn-site.xml"
        };
        Configuration conf = new Configuration();
        for (int i = 0; i < hadoopConfArr.length; i++) {
            conf.addResource(new Path(hadoopConfArr[i]));
        }

        Job job = Job.getInstance(conf, "WordCountJobHere");

        job.setJarByClass(WordCountMain.class);

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        FileInputFormat.addInputPath(job,new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        FileInputFormat.addInputPath(job, new Path("/hellodir/count"));
        FileOutputFormat.setOutputPath(job, new Path("/wcout"));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
