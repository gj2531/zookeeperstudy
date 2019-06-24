package mapreducewordcount;

import org.apache.commons.io.output.TeeOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(super.getConf(), JobMain.class.getSimpleName());
        //要想打包发送到服务器上执行必须加这句话
        job.setJarByClass(JobMain.class);
        //第一步 读取输入文件解析成key,value对
        job.setInputFormatClass(TextInputFormat.class);
        //打包到集群上时的路径
        //TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount"));
        //本地执行时的路径
        TextInputFormat.addInputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\3、大数据离线第三天\\wordcount\\input"));
        //第二步设置mapper类
        job.setMapperClass(WordCountMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        //第三步到第六步省略
        //第七步 设置reduce类
        job.setReducerClass(WordCountReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        //第八步 设置输出类以及输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
        //打包到集群上时的路径
        //TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount_out"));
        //本地执行时的路径
        TextOutputFormat.setOutputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\3、大数据离线第三天\\wordcount\\output"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Tool tool = new JobMain();
        int run = ToolRunner.run(configuration, tool, args);
        System.exit(run);
    }
}
