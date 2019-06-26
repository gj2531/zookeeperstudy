package partitiondemo;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PartitionerOwn extends Partitioner<Text,NullWritable> {
    @Override
    public int getPartition(Text text, NullWritable nullWritable, int i) {
        String[] result = text.toString().split("\t");
        String gameResult = result[5];
        if (null != gameResult && "" != gameResult){
            if (Integer.parseInt(gameResult) > 15){
                //如果结果大于15，这些大于15的数据都去到0号分区里边
                return 0;
            }else {
                //如果结果小于15,那么所有小于等于15的数据都去到一号分区里边
                return 1;
            }
        }
        return 0;
    }
}
