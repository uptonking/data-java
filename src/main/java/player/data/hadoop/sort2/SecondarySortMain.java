package player.data.hadoop.sort2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by yaoo on 2/14/18
 */
public class SecondarySortMain {
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

        Job job = Job.getInstance(conf, "SecondarySortJobHere");

        job.setJarByClass(SecondarySortMain.class);

        //FileInputFormat.addInputPath(job,new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path("/hellodir/sort2"));
        //job.setInputFormatClass(KeyValueTextInputFormat.class);


        job.setMapperClass(SecondarySortMapper.class);
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setPartitionerClass(FirstKeyPartitioner.class);
        job.setNumReduceTasks(3);

        //Map的输出，先分区，再排序分组
        job.setGroupingComparatorClass(GroupingComparator.class);
        //设置自定义比较策略(因为我的CombineKey重写了compareTo方法，所以这个可以省略)
        //job.setSortComparatorClass(DefinedComparator.class);

        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //FileOutputFormat.setOutputPath(job,new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("/sort2out"));
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
