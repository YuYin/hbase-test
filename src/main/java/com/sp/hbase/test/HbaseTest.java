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
            //1.��������ļ�����
            Configuration conf = HBaseConfiguration.create();
            //�������ò���:zk��ַ
            conf.set("hbase.zookeeper.quorum", "192.168.32.54");
            //2.��������
            con = ConnectionFactory.createConnection(conf);
            //3.��ûỰ
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
        //������������
        //a.�ж����ݿ��Ƿ����
        if (admin.tableExists(tn)) {
            System.out.println("====> ����ڣ�ɾ����....");
            //��ʹ������Ϊ���ɱ༭���رձ�
            admin.disableTable(tn);
            //ɾ����
            admin.deleteTable(tn);
            System.out.println("��ɾ���ɹ�.....");
        }
        //������ṹ����
        HTableDescriptor htd = new HTableDescriptor(tn);
        //��������ṹ����
        HColumnDescriptor hcd1 = new HColumnDescriptor("fm1");
        HColumnDescriptor hcd2 = new HColumnDescriptor("fm2");
        htd.addFamily(hcd1);
        htd.addFamily(hcd2);
        //������
        admin.createTable(htd);
        System.out.println("�������.....");
    }

    static void put() throws IOException {
        //��������
        Put put = new Put(Bytes.toBytes("row01"));//�������н�row01
        put.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col1"), Bytes.toBytes("value01"));

        //��ñ����
        Table table = con.getTable(tn);
        table.put(put);
        System.out.println("�����¼���.....");
    }

    static void batchPut() throws IOException {
        Put put01 = new Put(Bytes.toBytes("row02"));//�������н�
        put01.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col2"), Bytes.toBytes("value02"))
                .addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col3"), Bytes.toBytes("value03"));

        Put put02 = new Put(Bytes.toBytes("row03"));//�������н�
        put02.addColumn(Bytes.toBytes("fm1"), Bytes.toBytes("col4"), Bytes.toBytes("value04"));

        List<Put> puts = Arrays.asList(put01, put02);

        //��ñ����
        Table table = con.getTable(tn);
        table.put(puts);
    }

    static void scan() throws IOException {
        Scan scan = new Scan();
        //��ñ����
        Table table = con.getTable(tn);
        //�õ�ɨ��Ľ����
        ResultScanner rs = table.getScanner(scan);
        for (Result result : rs) {
            //�õ���Ԫ�񼯺�
            List<Cell> cs = result.listCells();
            for (Cell cell : cs) {
                //ȡ�н�
                String rowKey = Bytes.toString(CellUtil.cloneRow(cell));
                //ȡ��ʱ���
                long timestamp = cell.getTimestamp();
                //ȡ������
                String family = Bytes.toString(CellUtil.cloneFamily(cell));
                //ȡ��������
                String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                //ȡ��ֵ
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
            String rowKey = Bytes.toString(CellUtil.cloneRow(cell));  //ȡ�м�
            long timestamp = cell.getTimestamp();  //ȡ��ʱ���
            String family = Bytes.toString(CellUtil.cloneFamily(cell));  //ȡ������
            String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));  //ȡ��������
            String value = Bytes.toString(CellUtil.cloneValue(cell));  //ȡ��ֵ

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
