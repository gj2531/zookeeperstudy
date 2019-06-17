package zoodemo1;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.Test;

public class ZkTest1 {

    /*
    * 创建永久节点
    * */
    @Test
    public void createNode() throws Exception {
        RetryPolicy retryPolicy= new ExponentialBackoffRetry(1000, 1);
        //获取客户端对象
        CuratorFramework client = CuratorFrameworkFactory.newClient
                ("192.168.140.129:2181,192.168.140.130:2181,192.168.140.131:2181",
                        1000,1000,retryPolicy);
        //开启客户端
        client.start();
        //创建永久节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/hello/world");
        //关闭客户端
        client.close();
    }

    /*
    * 创建临时节点
    * */
    @Test
    public void createNode2() throws Exception {
        //获取客户端对象
        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.140.129:2181,192.168.140.130:2181,192.168.140.131:2181",
                1000,1000,new ExponentialBackoffRetry(1000,1));
        //开启客户端
        client.start();
        //创建临时节点
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath("/TempNode","world".getBytes());
        //设置线程等待
        Thread.sleep(20000);
        //关闭
        client.close();
    }

    /*
    * 修改节点数据
    * */
    @Test
    public void set() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.140.129:2181",
                1000,1000,new ExponentialBackoffRetry(1000,1));
        client.start();
        client.setData().forPath("/hello","can you speak chinese?".getBytes());
        client.close();
    }

    /*
    * 获取节点的数据
    *
    * */
    @Test
    public void get() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.140.129:2181", 1000, 1000,
                new ExponentialBackoffRetry(1000, 1));
        client.start();
        byte[] bytes = client.getData().forPath("/hello");
        System.out.println(new String(bytes));
        client.close();
    }

    /*
    * zk的watch机制
    * */
    @Test
    public void watchNode() throws Exception {
        //1.获取客户端
        final CuratorFramework client = CuratorFrameworkFactory.newClient("192.168.140.129:2181", 1000, 1000,
                new ExponentialBackoffRetry(1000, 3));
        //2.开启客户端
        client.start();
        //3.设置节点的cache
        TreeCache treeCache = new TreeCache(client, "/hello");
        //4.设置监听器和处理过程
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if (null != data){
                    switch (treeCacheEvent.getType()){
                        case NODE_ADDED:
                            //新增节点抓到了
                            System.out.println("节点增加了");
                            break;
                        case NODE_REMOVED:
                            System.out.println("节点移除了");
                            //删除节点
                            break;
                        case NODE_UPDATED:
                            System.out.println("节点更新了");
                            //修改节点
                            break;
                        default:
                            System.out.println("什么也不做");
                    }
                }
            }
        });
        treeCache.start();
        Thread.sleep(800000000);
    }
}
