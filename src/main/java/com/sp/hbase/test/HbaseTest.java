/*
 * www.zdsoft.cn Inc.
 * Copyright (c) 2005-2017 All Rights Reserved.
 */


package com.sp.hbase.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * @author <a href=mailto:someharder@gmail.com>yinyu</a> 2019/8/8
 */
public class HbaseTest {
    static Admin admin = null;
    static Connection con = null;
    static TableName tn = TableName.valueOf("hbaseTest");

    static {

        try {
            //1.获得配置文件对象
            Configuration conf = HBaseConfiguration.create();
            //设置配置参数:zk地址
            conf.set("hbase.zookeeper.quorum", "192.168.32.54");
            //2.建立连接
            con = ConnectionFactory.createConnection(conf);
            //3.获得会话
            admin = con.getAdmin();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws IOException {
        createTable();
        put();
        batchPut();
        scan();
        get();
        delete();

    }

    static void createTable() throws IOException {
        //创建表名对象
        //a.判断数据库是否存在
        if (admin.tableExists(tn)) {
            System.out.println("====> 表存在，删除表....");
            //先使表设置为不可编辑，关闭表
            admin.disableTable(tn);
            //删除表
            admin.deleteTable(tn);
            System.out.println("表删除成功.....");
        }
        //创建表结构对象
        HTableDescriptor htd = new HTableDescriptor(tn);
        //创建列族结构对象
        HColumnDescriptor hcd1 = new HColumnDescriptor("fm1");
        HColumnDescriptor hcd2 = new HColumnDescriptor("fm2");
        htd.addFamily(hcd1);
        htd.addFamily(hcd2);
        //创建表
        admin.createTable(htd);
        System.out.println("表创建完成.....");
    }

    static void put() throws IOException {
        //单个插入
        Put put = new Put(Bytes.toBytes("row01"));//参数是行健row01
        put.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col1"), Bytes.toBytes("value01"));

        //获得表对象
        Table table = con.getTable(tn);
        table.put(put);
        System.out.println("插入记录完成.....");
    }

    static void batchPut() throws IOException {
        Put put01 = new Put(Bytes.toBytes("row02"));//参数是行健
        put01.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col2"), Bytes.toBytes("value02"))
                .addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col3"), Bytes.toBytes("value03"));

        Put put02 = new Put(Bytes.toBytes("row03"));//参数是行健
        put02.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col4"), Bytes.toBytes("value04"));

        List<Put> puts = Arrays.asList(put01, put02);

        //获得表对象
        Table table = con.getTable(tn);
        table.put(puts);
    }

    static void scan() throws IOException {
        Scan scan = new Scan();
        //获得表对象
        Table table = con.getTable(tn);
        //得到扫描的结果集
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            //得到单元格集合
            List<Cell> cs = result.listCells();
            for (Cell cell : cs) {
                //取行健
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                //取到时间戳
                long timestamp = cell.getTimestamp();
                //取到族列
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                //取到修饰名
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                //取到值
                String value = Bytes.toString(CellUtil.cloneValue(cell));

                System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +
                        timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
            }
        }
    }

    static void get() throws IOException {
        Get get = new Get(Bytes.toBytes("row01"));
        get.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col2"));
        Table table = con.getTable(tn);
        Result r = table.get(get);
        List<Cell> cs = r.listCells();
        for (Cell cell : cs) {
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //取行键
            long timestamp = cell.getTimestamp();  //取到时间戳
            String family = Bytes.toString(CellUtil.cloneFamily(cell));  //取到族列
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));  //取到修饰名
            String value = Bytes.toString(CellUtil.cloneValue(cell));  //取到值

            System.out.println(" ===> rowKey : " + rowKey + ",  timestamp : " +
                    timestamp + ", family : " + family + ", qualifier : " + qualifier + ", value : " + value);
        }
    }

    static void delete() throws IOException {
        Delete delete = new Delete(Bytes.toBytes("row02"));
        delete.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col2"));
        Table table = con.getTable(tn);
        table.delete(delete);
    }
}
