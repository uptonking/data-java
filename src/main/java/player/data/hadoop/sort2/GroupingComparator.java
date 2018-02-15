package player.data.hadoop.sort2;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * 自定义分组函数
 * <p>
 * 分组是将相同键的值迭代成一个列表，就是将相同的键的记录整理成一组记录
 * <p>
 * 在reduce阶段，会构造一个key对应的value迭代器，同一个reduce里面具有相同key的数据会被分到同一个组
 * 分组函数，注意这个分组函数的排序排的是对应的组，而不是结果
 * <p>
 * 实现方法一种是继承WritableComparator，另外一种是实现RawComparator接口
 * <p>
 * 这里根据first分组
 * <p>
 * Created by yaoo on 2/14/18
 */
public class GroupingComparator extends WritableComparator {

    ///注释掉构造函数会报空指针
    // super(CompositeKey.class)会导致key1和key2为null
    protected GroupingComparator() {
       super(CompositeKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        CompositeKey k1 = (CompositeKey) a;
        CompositeKey k2 = (CompositeKey) b;

        return k1.getFirst().compareTo(k2.getFirst());
    }

}
