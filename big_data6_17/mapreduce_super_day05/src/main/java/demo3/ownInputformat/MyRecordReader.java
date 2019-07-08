package demo3.ownInputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class MyRecordReader extends RecordReader<NullWritable,BytesWritable> {
    private FileSplit fileSplit;
    private Configuration configuration;
    private BytesWritable bytesWritable = new BytesWritable();
    private boolean process = false;

    /**
     * 初始化调用一次
     * @param inputSplit 文件的切片 文件的内容都在切片中
     * @param context 上下文对象 通过上下文对象可以获取我们文件的一些配置信息
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit= (FileSplit) inputSplit;
        this.configuration=context.getConfiguration();
    }

    /**
     * 这个方法决定了我们怎么读取文件
     * 读取完成之后返回true
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!process){
            //在这个方法中我们需要读取文件 到一个byte[]中 然后把这个byte[]塞到BytesWritable中
            //获取切片文件,获取文件路径
            Path path = fileSplit.getPath();
            //需要获取filesystem对象，使用fileSystem对象获取文件的输入流
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream inputStream = fileSystem.open(path);
            //需要创建一个字节数组 用来存放我们读取的数据
            //Byte[] bytes = new Byte[(int) fileSplit.getLength()];
            byte[] bytes = new byte[(int) fileSplit.getLength()];
            //使用IOUtils工具类 把我们读取的数据 写入byte[]中
            IOUtils.readFully(inputStream, bytes,0,(int)fileSplit.getLength());
            //把我们的字节数组 全部塞到 BytesWritable中
            bytesWritable.set(bytes,0, (int) fileSplit.getLength());
            process = true;
            //关闭流
            IOUtils.closeStream(inputStream);
            //关闭filesyatem客户端对象
            fileSystem.close();
            return true;
        }
        return false;
    }

    /**
     * 这个方法获取我们的k1
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * 这个方法获取我们的v1
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return bytesWritable;
    }

    /**
     * 获取读取文件的进度
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    /**
     * 关闭
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
