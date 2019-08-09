/*
 * www.zdsoft.cn Inc.
 * Copyright (c) 2005-2017 All Rights Reserved.
 */
package com.sp.hbase.test.filter;

import com.sp.hbase.test.weibo.Constants;
import com.sp.hbase.test.weibo.DriverUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;

import java.io.IOException;
import java.util.List;

/**
 * @author <a href=mailto:someharder@gmail.com>yinyu</a> 2019/8/9
 * 行filter
 */
public class HbaseFilterTest {

    public static void main(String args[]) throws IOException{
        Table table = DriverUtils.getConnection().getTable(TableName.valueOf(Constants.TABLE_RELATION));
        Scan scan = new Scan();
        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.GREATER, new BinaryComparator("0001".getBytes()));
        scan.setFilter(rowFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        for(Result result : resultScanner) {
            List<Cell> cells = result.listCells();
            for(Cell cell : cells) {
                System.out.println(cell);
            }
        }
    }
}
