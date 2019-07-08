package demo4.ownOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormat extends FileOutputFormat<Text,NullWritable> {
    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        Configuration configuration = taskAttemptContext.getConfiguration();
        FileSystem fileSystem = FileSystem.get(configuration);
        FSDataOutputStream goodComm = fileSystem.create(new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义outputformat\\goodcomms\\good.txt"));
        FSDataOutputStream badComm = fileSystem.create(new Path("file:///G:\\大数据5期\\hadoop\\Hadoop课程资料\\5、大数据离线第五天\\自定义outputformat\\badcomms\\bad.txt"));

        MyRecordWriter myRecordWriter = new MyRecordWriter(goodComm,badComm);
        return myRecordWriter;
    }
}
