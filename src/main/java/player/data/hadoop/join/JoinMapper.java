package player.data.hadoop.join;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by yaoo on 2/14/18
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, Text> {

    //name-id
    public final String LEFT_FILENAME = "student_info.txt";
    //id-class
    public final String RIGHT_FILENAME = "student_class.txt";
    public final String LEFT_FILENAME_FLAG = "left";
    public final String RIGHT_FILENAME_FLAG = "right";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String filePath = ((FileSplit) context.getInputSplit()).getPath().toString();
        String fileFlag = null;
        String joinKey = null;
        String joinValue = null;


        if (filePath.contains(LEFT_FILENAME)) {
            fileFlag = LEFT_FILENAME_FLAG;

            //要比较匹配的字段作为key
            joinKey = value.toString().split(" ")[1];
            joinValue = value.toString().split(" ")[0];
        }

        if (filePath.contains(RIGHT_FILENAME)) {
            fileFlag = RIGHT_FILENAME_FLAG;

            //要比较匹配的字段作为key
            joinKey = value.toString().split(" ")[0];
            joinValue = value.toString().split(" ")[1];
        }

        //输出id, name left / class right
        context.write(new Text(joinKey), new Text(joinValue + " " + fileFlag));

    }

}
