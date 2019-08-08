/*
 * www.zdsoft.cn Inc.
 * Copyright (c) 2005-2017 All Rights Reserved.
 */
package com.sp.hbase.test.weibo;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author <a href=mailto:someharder@gmail.com>yinyu</a> 2019/8/8
 */
public class TableCreator {

     /**
     * 创建微博内容表
     * Table Name:content
     * RowKey:用户 ID_时间戳
     * ColumnFamily:info
     * ColumnLabel:标题,内容,图片 URL
     * Version:1 个版本
     */
    public void createTableContent() {
        try {
           //创建表表述
            HTableDescriptor contentTableDescriptor = new
                    HTableDescriptor(TableName.valueOf("content"));
           //创建列族描述
            HColumnDescriptor infoColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("info"));
            //设置块缓存
            infoColumnDescriptor.setBlockCacheEnabled(true);
           //设置块缓存大小
            infoColumnDescriptor.setBlocksize(2097152);
              //设置压缩方式
           // infoColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
          //设置版本确界
            infoColumnDescriptor.setMaxVersions(1);
            infoColumnDescriptor.setMinVersions(1);
            contentTableDescriptor.addFamily(infoColumnDescriptor);
            DriverUtils.getAdmin().createTable(contentTableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
     /*       if (null != admin) {
                try {
                    admin.close();
                    con.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    /**
     * 用户关系表
     * Table Name:relation
     * RowKey:用户 ID
     * ColumnFamily:attends,fans
     * ColumnLabel:关注用户 ID，粉丝用户 ID
     * ColumnValue:用户 ID
     * Version：1 个版本
     */
    public void createTableRelation() {
        try {
            HTableDescriptor relationTableDescriptor = new HTableDescriptor(TableName.valueOf("relation"));
            //关注的人的列族
            HColumnDescriptor attendColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("attends"));
           //设置块缓存
            attendColumnDescriptor.setBlockCacheEnabled(true);
            //设置块缓存大小
            attendColumnDescriptor.setBlocksize(2097152);
            //设置压缩方式
          //attendColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
            //设置版本确界
            attendColumnDescriptor.setMaxVersions(1);
            attendColumnDescriptor.setMinVersions(1);
            //粉丝列族
            HColumnDescriptor fansColumnDescriptor = new
                    HColumnDescriptor(Bytes.toBytes("fans"));
            fansColumnDescriptor.setBlockCacheEnabled(true);
            fansColumnDescriptor.setBlocksize(2097152);
            fansColumnDescriptor.setMaxVersions(1);
            fansColumnDescriptor.setMinVersions(1);
            relationTableDescriptor.addFamily(attendColumnDescriptor);
            relationTableDescriptor.addFamily(fansColumnDescriptor);
            DriverUtils.getAdmin().createTable(relationTableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
           /* if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }*/
        }
    }

    /**
     * 创建微博收件箱表
     * Table Name:inbox
     * RowKey:用户 ID
     * ColumnFamily:info
     * ColumnLabel:用户 ID_发布微博的人的用户 ID
     * ColumnValue:关注的人的微博的 RowKey
     * Version:1000
     */
    public void createTableInbox() {
        try {
            HTableDescriptor inboxTableDescriptor = new HTableDescriptor(TableName.valueOf("inbox"));
            HColumnDescriptor infoColumnDescriptor = new
                    HColumnDescriptor(Bytes.toBytes("info"));
            infoColumnDescriptor.setBlockCacheEnabled(true);
            infoColumnDescriptor.setBlocksize(2097152);
            infoColumnDescriptor.setMaxVersions(1000);
            infoColumnDescriptor.setMinVersions(1000);
            inboxTableDescriptor.addFamily(infoColumnDescriptor);
            DriverUtils.getAdmin().createTable(inboxTableDescriptor);
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
       /*     if (null != admin) {
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }*/
        }
    }
}
