package demo5.grouping;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GroupingMap extends Mapper<LongWritable,Text,OrderBean,NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        OrderBean orderBean = new OrderBean();
        orderBean.setOrderId(split[0]);
        orderBean.setPrice(Double.parseDouble(split[2]));
        context.write(orderBean,NullWritable.get());
    }
}
