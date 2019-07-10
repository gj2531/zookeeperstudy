package demo6.groupingN;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class GroupingReduce extends Reducer<OrderBean,Text,Text,Text> {
    @Override
    protected void reduce(OrderBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int i = 0;

        for (Text value : values) {
            if (i >= 2){
                break;
            }
            i++;
            String[] split = value.toString().split("\t");
            context.write(new Text(key.getOrderId()),new Text(split[2]));

        }
    }
}
