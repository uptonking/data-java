package player.data.hadoop.sort2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 自定义键
 * <p>
 * 两次排序的规则
 * <p>
 * Created by yaoo on 2/14/18
 */
public class CompositeKey implements WritableComparable<CompositeKey> {

    private String first;
    private Integer second;

    public CompositeKey() {
        this.first = new String();
        this.second = new Integer(0);
    }

    public CompositeKey(String first, Integer second) {
        this.first = first;
        this.second = second;
    }

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public Integer getSecond() {
        return second;
    }

    public void setSecond(Integer second) {
        this.second = second;
    }

    @Override
    public void write(DataOutput out) throws IOException {

        out.writeUTF(this.first);
        out.writeInt(this.second);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.first = in.readUTF();
        this.second = in.readInt();
    }

    /**
     * 自定义比较策略
     *
     * @param o 第2个比较对象
     * @return 0/1/-1
     */
    @Override
    public int compareTo(CompositeKey o) {

        int cmp1 = this.first.compareTo(o.first);

        if (cmp1 != 0) {
            return cmp1;
        }

        return this.second.compareTo(o.second);
    }

    @Override
    public int hashCode() {

        //取一个质数来计算hash值
        final int prime = 31;
        int h = 1;
        h = prime * h + (first == null ? 0 : first.hashCode());
        return h;
    }

    @Override
    public boolean equals(Object obj) {

        if (obj == this) {
            return true;
        }

        if (obj == null) {
            return false;
        }

        if (obj.getClass() != this.getClass()) {
            return false;
        }

        CompositeKey other = (CompositeKey) obj;
        if (first.equals(other.first) && second.equals(other.second)) {
            return true;
        }

        return false;
    }
}
