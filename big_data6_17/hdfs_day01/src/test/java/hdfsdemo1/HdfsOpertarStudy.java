package hdfsdemo1;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.testng.annotations.Test;

import java.io.*;
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

    /*
    * 获取hdfs文件系统下所有的文件
    * */
    @Test
    public void getAllHdfsFilePath() throws URISyntaxException, IOException {
        //获取hdfs分布式文件客户端
        FileSystem fileSystem = FileSystem.newInstance(new URI("hdfs://node01:8020"), new Configuration());
        //获取根目录下的所有文件和文件夹
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        //遍历获得的文件路径集合
        for (FileStatus fileStatus : fileStatuses) {
            //判断是否是文件夹，如果是就递归查询，不是就输出文件路径
            if(fileStatus.isDirectory()){
                getDirectorFile(fileSystem,fileStatus);
            }else {
                Path path = fileStatus.getPath();
                System.out.println(path.toString());
            }
        }
        //关闭客户端
        fileSystem.close();
    }
    /*
    * 递归获取文件夹下文件的方法
    * */
    private void getDirectorFile(FileSystem fileSystem, FileStatus fileStatus) throws IOException {
        FileStatus[] fileStatuses = fileSystem.listStatus(fileStatus.getPath());
        for (FileStatus status : fileStatuses) {
            if (status.isDirectory()){
                getDirectorFile(fileSystem,status);
            }else {
                System.out.println(status.getPath().toString());
            }
        }
    }
    
    /*
    * 使用listFiles遍历hgfs上面所有的文件
    * */
    @Test
    public void getHdfsPath() throws URISyntaxException, IOException {
        //获取hdfs分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //使用listFiles获取所有文件的迭代器
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        //使用while循环遍历迭代器
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            Path path = next.getPath();
            System.out.println(path.toString());
        }
    }
    /*
    * 从hdfs上下载文件到本地
    * 通过流的形式下载
    * */
    @Test
    public void copyFileFromHdfs() throws Exception{
        //获取hdfs分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //获取输入流对象
        FSDataInputStream inputStream = fileSystem.open(new Path("/test/input/install.log"));
        //获取输出流对象
        FileOutputStream outputStream = new FileOutputStream(new File("D:\\myinstall.log"));

        IOUtils.copy(inputStream,outputStream);

        IOUtils.closeQuietly(inputStream);
        IOUtils.closeQuietly(outputStream);
        fileSystem.close();
    }
    /*
     * 从hdfs上下载文件到本地
     * 通过copyLocalFile的形式下载
     *
     * 待修改
     * */
    @Test
    public void copyFileFromHdfs2() throws Exception{
        //获取hdfs分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //使用copyLocalFile从hdfs上下载文件
        fileSystem.copyToLocalFile(new Path("/test/input/install.log"),new Path("file:///D:\\myins.log"));
        fileSystem.close();
    }

    /*
    * 在hdfs上创建文件夹
    * */
    @Test
    public void mkdirDir() throws Exception{
        //获取hdfs分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //创建文件夹
        fileSystem.mkdirs(new Path("/a/b/c"));
        //关闭客户端
        fileSystem.close();
    }

    /*
    *hdfs文件的上传
    * 通过copyFromLocalFile上传文件
    * */
    @Test
    public void copyToHdfsFile() throws Exception{
        //获取hdfs分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //上传文件到hdfs
        fileSystem.copyFromLocalFile(false,new Path("file:///d:\\myinstall.log"),new Path("/a/b/c"));
        //关闭hdfs客户端
        fileSystem.close();
    }

    /*
     *hdfs文件的上传
     * 通过流的形式上传文件
     * */
    @Test
    public void copyToHdfsFile2() throws Exception{
        //获取hdfs分布式文件系统的客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //获取输出流对象
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/a/b/c/a.txt"));
        //获取输入流对象读取本地文件系统的文件
        FileInputStream fileInputStream = new FileInputStream(new File("D:\\myinstall.log"));
        //
        IOUtils.copy(fileInputStream,fsDataOutputStream);
        //关闭流
        IOUtils.closeQuietly(fsDataOutputStream);
        IOUtils.closeQuietly(fileInputStream);
        //关闭客户端
        fileSystem.close();
    }

    /*
    * hdfs的权限校验机制
    * */
    @Test
    public void hdfsJiaoYan() throws Exception{
        //获取hdfs分布式文件系统的客户端对象
        //这种方法不太行，只有root用户有权限，所以需要伪造用户
        //FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
        //伪造身份下载文件
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration(), "root");
        //通过copytolocalfile方法从hdfs下载文件
        fileSystem.copyToLocalFile(new Path("/config/core-site.xml"),new Path("file:///D:\\a.xml"));
        //关闭客户端
        fileSystem.close();
    }
}
