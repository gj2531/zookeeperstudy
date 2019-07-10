package demo1.friendComm.Step2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Step2Reducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuffer buffer = new StringBuffer();
        for (Text value : values) {
            buffer.append(value.toString()+"-");
        }
        context.write(key,new Text(buffer.toString()));
    }
}
