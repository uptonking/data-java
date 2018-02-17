package player.data.hadoop.join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * 实际的Join操作由reducer完成
 * <p>
 * Created by yaoo on 2/14/18
 */
public class JoinReducer extends Reducer<Text, Text, Text, Text> {

    //name-id
    public final String LEFT_FILENAME = "student_info.txt";
    //id-class
    public final String RIGHT_FILENAME = "student_class.txt";
    public final String LEFT_FILENAME_FLAG = "left";
    public final String RIGHT_FILENAME_FLAG = "right";

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> it = values.iterator();
        List<String> stuClass = new ArrayList<>();
        String stuName = "";

        while (it.hasNext()) {

            String[] info = it.next().toString().split(" ");

            if (info[1].equals(LEFT_FILENAME_FLAG)) {
                stuName = info[0];
            }

            if (info[1].equals(RIGHT_FILENAME_FLAG)) {
                stuClass.add(info[0]);
            }
        }

        //笛卡尔积
        for (int i = 0; i < stuClass.size(); i++) {
            context.write(new Text(stuName), new Text(stuClass.get(i)));
        }
    }
}
