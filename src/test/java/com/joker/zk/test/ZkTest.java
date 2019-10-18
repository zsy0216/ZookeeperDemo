package com.joker.zk.test;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;

/**
 * @PackageName:com.joker.zk.test
 * @Date:2019/10/18 8:33
 * @Author: zsy
 */
public class ZkTest {
    // 连接Zookeeper服务的对象
    ZooKeeper zooKeeper;
    // 连接信息
    String connectString = "192.168.252.128:2181";
    // 连接超时时间 ms
    int sessionTimeOut = 5000;

    Watcher watcher = new Watcher() {
        @Override
        public void process(WatchedEvent watchedEvent) {
        }
    };

    {
        try {
            zooKeeper = new ZooKeeper(connectString, sessionTimeOut, watcher);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpdateNodeData() throws KeeperException, InterruptedException {
        // 要操作的节点的路径
        String path = "/animal/cat";
        // 获取当前节点值
        byte[] resultByteArray = zooKeeper.getData(path, false, new Stat());
        // 将字节数组封装为字符串，输出
        String result = new String(resultByteArray);
        System.out.println(result);

        // 获取新值字符串对应的字节数组
        byte[] newValueByteArray = new String("mimi").getBytes();
        // 指定当前操作所基于的版本号，不确定可使用-1
        int version = -1;
        // 执行节点值的修改
        Stat stat = zooKeeper.setData(path, newValueByteArray, version);
        // 获取最新的版本号
        int newVersion = stat.getVersion();
        System.out.println("newVersion=" + newVersion);

        //获取最新的值，输出
        resultByteArray = zooKeeper.getData(path, false, new Stat());
        System.out.println(new String(resultByteArray));
        zooKeeper.close();
    }

    @Test
    public void testNoticeOnce() throws KeeperException, InterruptedException {
        // 要操作的节点路径
        String path = "/animal/cat";
        Watcher watcher = new Watcher() {
            // 当前Watcher检测到节点值的变化，会调用process方法
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.err.println("接收到了通知，值发生了修改！");
            }
        };
        // 修改前的值
        byte[] oldValue = zooKeeper.getData(path, watcher, new Stat());
        System.out.println("oldValue=" + oldValue);

        // 持续运行程序，等待Zookeeper修改值进行异步通知
        while (true) {
            Thread.sleep(5000);
            System.err.println("当前方法要执行的业务逻辑");
        }
    }


    @Test
    public void testNoticeForever() throws KeeperException, InterruptedException {
        // 要操作的节点路径
        String path = "/animal/cat";
        getDataWithNotice(zooKeeper, path);

        // 持续运行程序，等待Zookeeper修改值进行异步通知
        while (true) {
            Thread.sleep(5000);
            System.err.println("当前方法要执行的业务逻辑");
        }
    }

    public void getDataWithNotice(ZooKeeper zooKeeper, String path) throws KeeperException, InterruptedException {
        byte[] resultByteArray = zooKeeper.getData(path, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                // 以类似递归的方式调用getDataWithNotice()方法实现持续监控
                /**
                 * 这里不是真正的递归，这个process()方法是异步执行；
                 * 当getDataWithNotice()执行时，在创建完Watcher对象之后，继续执行到结束并释放资源
                 * 此时Watcher对象还在内存中，当接收到了修改时，再异步调用process()方法;
                 * 然后重新调用getDataWithNotice()方法，创建Watcher对象，实现持续监控。
                 * */
                try {
                    System.out.println("*接收到了修改*");
                    getDataWithNotice(zooKeeper, path);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, new Stat());

        String result = new String(resultByteArray);
        System.out.println("当前节点值=" + result);
    }
}
