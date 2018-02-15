package player.data.hadoop.sort2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by yaoo on 2/14/18
 */
public class SecondarySortMapper extends Mapper<LongWritable, Text, CompositeKey, IntWritable> {

    private CompositeKey compositeKey = new CompositeKey();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split(" ");
        String k1 = line[0];
        int k2 = Integer.parseInt(line[1]);

        compositeKey.setFirst(k1);
        compositeKey.setSecond(k2);

        context.write(compositeKey, new IntWritable(k2));

    }
}
