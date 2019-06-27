package flowsortdemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowSortMap extends Mapper<LongWritable,Text,FlowSortBean,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split("\t");
        FlowSortBean flowSortBean = new FlowSortBean();
        flowSortBean.setUpFlow(Integer.parseInt(split[1]));
        flowSortBean.setDownFlow(Integer.parseInt(split[2]));
        flowSortBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowSortBean.setDownCountFlow(Integer.parseInt(split[4]));
        context.write(flowSortBean,new Text(split[0]));
    }
}
