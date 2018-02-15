package player.data.hadoop.sort2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 如果reduce的输入与输出不是同一种类型，则 Combiner和Reducer 不能共用 Reducer 类，
 * 因为 Combiner 的输出是 reduce 的输入。除非重新定义一个Combiner。
 * Created by yaoo on 2/14/18
 */
public class SecondarySortReducer extends Reducer<CompositeKey, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(CompositeKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for (IntWritable val : values) {
            context.write(new Text(key.getFirst()), val);
        }

    }
}
