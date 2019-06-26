package pairsortdemo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class SortReduce extends Reducer<PairSort,Text,PairSort,NullWritable> {
    public static enum Counter{
        ZKDSB_KEY,
        ZKCSB_VALUE
    }

    @Override
    protected void reduce(PairSort key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.getCounter(Counter.ZKDSB_KEY).increment(1L);
        for (Text value : values) {
            context.getCounter(Counter.ZKCSB_VALUE).increment(1L);
            context.write(key,NullWritable.get());
        }
    }
}
