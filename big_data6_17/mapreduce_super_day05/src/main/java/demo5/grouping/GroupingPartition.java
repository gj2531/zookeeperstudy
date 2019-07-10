package demo5.grouping;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义我们的分区类 相同的orderId 发说说到同一个reduce里面去
 */
public class GroupingPartition extends Partitioner<OrderBean,NullWritable> {
    @Override
    public int getPartition(OrderBean orderBean, NullWritable nullWritable, int numReduceTask) {
        int i = (orderBean.getOrderId().hashCode() & Integer.MAX_VALUE) % numReduceTask;
        return i;

    }
}
