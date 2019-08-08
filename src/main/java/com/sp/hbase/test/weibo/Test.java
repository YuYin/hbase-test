/*
 * www.zdsoft.cn Inc.
 * Copyright (c) 2005-2017 All Rights Reserved.
 */
package com.sp.hbase.test.weibo;

import java.io.IOException;
import java.util.List;

/**
 * @author <a href=mailto:someharder@gmail.com>yinyu</a> 2019/8/8
 */
public class Test {

    /**
     * 发布微博内容
     * 添加关注
     * 取消关注
     * 展示内容
     */
    public static void testPublishContent(Biz wb) throws IOException {
        wb.publishContent("0001", "xxxxxxxxxxxxxxxxxxxxxx");
        wb.publishContent("0001", "今天天气不错。");
    }

    public static void testAddAttend(Biz wb) throws IOException {
        wb.publishContent("0008", "准备下课！");
        wb.publishContent("0009", "准备关机！");
        wb.addAttends("0001", "0008", "0009");
    }

    public static void testRemoveAttend(Biz wb) throws IOException {
        wb.removeAttends("0001", "0008");
    }

    public static void testShowMessage(Biz wb) throws IOException{
        List<Message> messages = wb.getAttendsContent("0001");
        for (Message message : messages) {
            System.out.println(message);
        }
    }

    public static void main(String[] args) throws IOException {
        Biz biz = new Biz();
        TableCreator tableCreator = new TableCreator();
        tableCreator.createTableContent();
        tableCreator.createTableInbox();
        tableCreator.createTableRelation();

        testPublishContent(biz);
        testAddAttend(biz);
        testShowMessage(biz);
        testRemoveAttend(biz);
        testShowMessage(biz);
    }
}
