package demo1.friendComm.step1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Step1Main extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "xxx");
        //打包必须添加
        job.setJarByClass(Step1Main.class);
        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\共同好友\\input"));
        //第二步：设置mapper类
        job.setMapperClass(Step1Map.class);
        //设置泛型类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //第三步到第六步省略
        //第七步：设置reducer类
        job.setReducerClass(Step1Reducer.class);
        //设置泛型类型
        job.setOutputValueClass(Text.class);
        job.setOutputValueClass(Text.class);
        //第八步：输出数据
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\共同好友\\out1"));

        //提交
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new Step1Main(), args);
        System.exit(run);
    }
}
