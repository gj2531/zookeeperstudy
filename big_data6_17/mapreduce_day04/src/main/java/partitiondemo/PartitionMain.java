package partitiondemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PartitionMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //定义一个job类，组装我们的mr
        Job job = Job.getInstance(super.getConf(), PartitionMain.class.getSimpleName());
        //打包必须要加的
        job.setJarByClass(PartitionMain.class);
        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/partition_in"));
        //第二步：map类
        job.setMapperClass(PartitionerMap.class);
        //设置k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //第三步：设置分区类partition
        job.setPartitionerClass(PartitionerOwn.class);
        //设置reduceTask个数
        job.setNumReduceTasks(2);
        //第四步 排序 第五步 规约 第六步 分组
        //第七步 设置reduce类
        job.setReducerClass(PartitionReduce.class);
        //设置k3 v3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //第八步 设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/partition_out"));
        //提交任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new PartitionMain(), args);
        System.exit(run);
    }
}
