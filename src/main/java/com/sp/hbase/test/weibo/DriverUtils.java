/*
 * www.zdsoft.cn Inc.
 * Copyright (c) 2005-2017 All Rights Reserved.
 */
package com.sp.hbase.test.weibo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @author <a href=mailto:someharder@gmail.com>yinyu</a> 2019/8/8
 */
public class DriverUtils {

    public static Connection getConnection() {
            //1.获得配置文件对象
            Configuration conf = HBaseConfiguration.create();
            //设置配置参数:zk地址
            conf.set("hbase.zookeeper.quorum", Constants.ZK_ADDRESS);
        try {
                //2.建立连接
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException("获取hbase连接失败");
        }
    }

    public static Admin getAdmin()  {
        try {
            return getConnection().getAdmin();
        } catch (IOException e) {
             throw new RuntimeException("获取admin会话失败");
        }
    }
}
