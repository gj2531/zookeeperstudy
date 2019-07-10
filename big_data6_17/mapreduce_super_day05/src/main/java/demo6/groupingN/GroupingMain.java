package demo6.groupingN;

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

public class GroupingMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "xxx");
        //打包运行必须要添加
        job.setJarByClass(GroupingMain.class);
        //第一步：读取文件 k1 v1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义groupingComparator\\input"));
        //第二步：自定义mapper类
        job.setMapperClass(GroupingMap.class);
        //设置泛型类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(Text.class);
        //第三步：设置分区类
        job.setPartitionerClass(GroupingPartition.class);
        //第四步、第五步省略
        //第六步：设置自定义分组类
        job.setGroupingComparatorClass(GroupingCompator.class);
        //第七步：设置自定义reducer类
        job.setReducerClass(GroupingReduce.class);
        //设置泛型类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //第八步：输出数据
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义groupingComparator\\out3"));

        //提交任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new GroupingMain(), args);
        System.exit(run);
    }
}
