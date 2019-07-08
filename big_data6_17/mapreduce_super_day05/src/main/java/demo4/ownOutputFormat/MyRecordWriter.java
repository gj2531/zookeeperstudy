package demo4.ownOutputFormat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class MyRecordWriter extends RecordWriter<Text,NullWritable> {
    public MyRecordWriter(){}
    private FSDataOutputStream goodComm;
    private FSDataOutputStream badComm;
    public MyRecordWriter(FSDataOutputStream goodComm,FSDataOutputStream badComm){
        this.goodComm=goodComm;
        this.badComm=badComm;
    }
    @Override
    public void write(Text text, NullWritable nullWritable) throws IOException, InterruptedException {
        String[] split = text.toString().split("\t");
        String s = split[9];
        if (Integer.parseInt(s) <= 1){
            //好评数据
            goodComm.write(text.toString().getBytes());
            goodComm.write("\r\n".getBytes());
        }else {
            //差评数据
            badComm.write(text.toString().getBytes());
            badComm.write("\r\n".getBytes());
        }
    }

    @Override
    public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        IOUtils.closeStream(goodComm);
        IOUtils.closeStream(badComm);
    }
}
