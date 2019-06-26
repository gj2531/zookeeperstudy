package pairsortdemo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMap extends Mapper<LongWritable,Text,PairSort,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        PairSort pairSort = new PairSort();
        String[] split = value.toString().split("\t");
        pairSort.setFirst(split[0]);
        pairSort.setSecond(Integer.parseInt(split[1]));
        context.write(pairSort,value);
    }
}
