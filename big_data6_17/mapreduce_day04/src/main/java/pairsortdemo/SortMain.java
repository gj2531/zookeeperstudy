package pairsortdemo;

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

public class SortMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //获取job对象
        Job job = Job.getInstance(super.getConf(), "xxx");
        //打包必须添加
        job.setJarByClass(SortMain.class);
        //第一步：读取文件
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\4、大数据离线第四天\\排序\\input"));
        //第二步：设置mapper类
        job.setMapperClass(SortMap.class);
        //设置泛型类型
        job.setMapOutputKeyClass(PairSort.class);
        job.setMapOutputValueClass(Text.class);
        //第三步到第六步省略
        //第五步：设置规约类
        job.setCombinerClass(MyCombiner.class);
        //第七步：设置reducer类
        job.setReducerClass(SortReduce.class);
        //设置泛型类型
        job.setOutputKeyClass(PairSort.class);
        job.setOutputValueClass(NullWritable.class);
        //第八步：输出
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\4、大数据离线第四天\\排序\\out3"));
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new SortMain(), args);
        System.exit(run);
    }
}
