package flowsortdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlowSortMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "xxx");
        //打包必须添加
        job.setJarByClass(FlowSortMain.class);
        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\4、大数据离线第四天\\流量统计\\out"));
        //第二步：设置mapper类
        job.setMapperClass(FlowSortMap.class);
        //设置泛型类型
        job.setMapOutputKeyClass(FlowSortBean.class);
        job.setMapOutputValueClass(Text.class);
        //第三到第六步省略
        //第七步：设置reducer类
        job.setReducerClass(FlowSortReduce.class);
        //设置泛型类型
        job.setOutputKeyClass(FlowSortBean.class);
        job.setOutputValueClass(Text.class);

        //第八步：输出数据
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\4、大数据离线第四天\\流量统计\\out_flow_sort"));

        //提交
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new FlowSortMain(), args);
        System.exit(run);
    }
}
