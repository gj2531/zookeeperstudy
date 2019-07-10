package demo1.friendComm.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Step1Map extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] split = value.toString().split(":");
        String[] split1 = split[1].split(",");
        for (String s : split1) {
            context.write(new Text(s),new Text(split[0]));
        }
    }
}
