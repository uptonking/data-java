package player.data.hadoop.sort2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 * <p>
 * 根据key的值决定value的输出的reducer
 * 自己根据key的分布规律确定分区规则
 * <p>
 * 默认分区会将map所有输出输入到同一个reducer
 * <p>
 * Created by yaoo on 2/14/18
 */
public class FirstKeyPartitioner extends Partitioner<CompositeKey, IntWritable> {

    /**
     * 根据输入数据的大小，分成3个区
     * <p>
     * 这里确定reducer的数量
     */
    @Override
    public int getPartition(CompositeKey compositeKey, IntWritable intWritable, int numPartitions) {

        int firstNum = Integer.parseInt(compositeKey.getFirst());

        if (firstNum <= 4) {
            return 0;
        } else if (firstNum <= 6) {
            return 1;
        } else {
            return 2;
        }
    }
}
