package hdfsdemo1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.testng.annotations.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
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

    /*
    * 获取分布式文件系统的第一种方式
    * */
    @Test
    public void getHdfs1() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020/");
        FileSystem fileSystem = FileSystem.get(configuration);
        System.out.println(fileSystem.toString());
    }

    /*
    * 获取分布式文件系统的第二种方式
    * */
    @Test
    public void getHdfs2() throws URISyntaxException, IOException {
        Configuration configuration = new Configuration();

        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020/"), configuration);
        System.out.println(fileSystem.toString());
    }

    /*
    * 获取分布式文件系统的第三种方式
    * */
    @Test
    public void getHdfs3() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://node01:8020/");
        FileSystem fileSystem = FileSystem.newInstance(configuration);
        System.out.println(fileSystem.toString());
    }

    /*
    * 获取分布式文件系统的第四种方式
    * */
    @Test
    public void getHdfs4() throws URISyntaxException, IOException {
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020/"), new Configuration());
        System.out.println(fileSystem.toString());
    }
}
