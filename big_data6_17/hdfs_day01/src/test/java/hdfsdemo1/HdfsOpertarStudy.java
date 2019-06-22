package hdfsdemo1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

public class HdfsOpertarStudy {
    /*
    * 这是一个hdfs以URL的方式读取虚拟机上的文件
    * */
    @Test
    public void getFile() throws IOException {
        URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
        InputStream inputStream = new URL("hdfs://192.168.140.129:8020/test/input/install.log").openStream();
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\hello.txt"));
        IOUtils.copy(inputStream,outputStream);
        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
    }
}
