package com.atguigu.zookeeper;
import javafx.scene.shape.Path;
import org.apache.log4j.net.SyslogAppender;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ZKClientTest {

    ZooKeeper zooKeeper;

    @Before
    public void before() throws IOException {
        String connect_string = "hadoop102:2181,hadoop105:2181,hadoop104:2181";
        //1.创建zookeeper对象
        /*第一个参数zookeeper地址  第二个客户端和服务端断开之后连接超时时间
         * 第三个*/
        zooKeeper = new ZooKeeper(
                connect_string,    //zookeeper地址
                2000,  //超时时间
                new Watcher() {          //定义回调函数(每次数据变化都会被包装成一个event即事件)
                    public void process(WatchedEvent event) {
                        System.out.println("默认的回调函数");
                        //如果在监听的时候没有再传回调函数，那么会默认调用这个,
                        // 并且在创建后，初始化阶段会调用一次，关闭资源时会再次调用
                    }
                }
        );

    }

    @After
    public void after() throws InterruptedException {
        //3.关闭资源
        zooKeeper.close();
    }


    @Test
    public void create() throws IOException, KeeperException, InterruptedException {
        //2.做事情
        /*第一个参数：要创建的节点  第二个：数据（byte数组） 第三个访问控制列表（预制了很多）
        * 第四个设置创建节点的类型*/
        zooKeeper.create(
                "/test001",
                "123".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,    // 表示都可以访问（777）
                CreateMode.PERSISTENT           //表示创建节点是永久的 并且无序的
        );

    }

    @Test   //修改
    public void set() throws KeeperException, InterruptedException {
        zooKeeper.setData(
                "/test001",  //要修改节点的路径
                "123".getBytes(),  //要修改为什么值
                  0        /*设置数据目前的版本，防止多个客户端同时对其进行修改，当多个客户端对其进行修改时，先
                                   到达的客户端修改完后，数据的版本号会自动+1，后面客户端的版本号不匹配，修改失败
                                   它是一种乐观锁，即大家都能够进行访问，但是只有一个能修改成功
                                    悲观锁，即有一个线程访问时，其他人都不可以对其进行访问*/
        );

    }

    @Test   //查询
    public void ls() throws KeeperException, InterruptedException {
        /*重载，也可以在后面传入一个Stat
           Stat stat = new Stat();  //将状态信息截取到stat对象里
           stat放在watch后面  可以使用get等方法获取具体信息。
         */

        //查询结果为list集合
        List<String> children = zooKeeper.getChildren(
                "test001",
                true    //传入一个布尔值，表示是否需要监听，调用默认的回调函数

                /*也可以new watcher()定制一个回调函数
                * new watcher(){
                *    public void process (WacthedEvent event){
                *     event.getPath().sout    //获取事件的父目录
                *     event.getType().sout    //获取事件的类型 node children changed 子节点发生改变
                *     event.getState().sout   //获取状态     Sync Connected
                *    }
                * }*/
        );

        for (String child : children) {
            System.out.println(child);
        }
    }

    @Test
    public void get() throws KeeperException, InterruptedException {
        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(
                "/test001",
                true,
                stat
        );
        System.out.println(data);
    }

    @Test
    public void delete() throws KeeperException, InterruptedException {

        //获取节点存在的相关信息
        Stat stat = zooKeeper.exists(
                "/test001",
                true
        );

        if(stat.getNumChildren()>0) {
        };

        //判断节点是否存在
        if(stat==null){
            System.out.println("节点不存在");
        }else{
            zooKeeper.delete("test001", stat.getVersion());
        }

    }

    //递归删除方法
    //递归删除整个节点，包括其子节点
    @Test
    public void deleteall(String path) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(
                path,
                true
        );

        if(stat==null){
            System.out.println("节点不存在");
        }else{

            if(stat.getNumChildren()>0){
                List<String> children = zooKeeper.getChildren(
                        path,
                        false
                );
                for (String child : children) {
                    deleteall(path+"/"+child);
                }

            }

            zooKeeper.delete(
                    path,
                    stat.getVersion()
            );
        }

    }
}
