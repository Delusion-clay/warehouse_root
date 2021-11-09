package com.cn.it.warehouse.canal;


import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalTest {
    public static void main(String[] args) {

        //创建连接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop-101", 11111),
                "shop", "root", "_Qq3pw34w9bqa");

        int batchSize = 1000;
        int emptyCount = 0;

        try {
            connector.connect();
            connector.subscribe(".*\\..*");
            connector.rollback();

            int totalEmptyCount = 120;
            //如果120次为空，就做相关处理
            while (emptyCount < totalEmptyCount){
                Message message = connector.getWithoutAck(batchSize);

                //批次ID
                long batchId = message.getId();
                //批次数量
                int size = message.getEntries().size();
                if(batchId==-1 || size == 0){
                    //如果批次为空，空加1
                    emptyCount++;
                    System.out.println("emptyCount:"+emptyCount);

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }else{
                    // 拿到数据
                    emptyCount = 0;
                    List<CanalEntry.Entry> entries = message.getEntries();
                   // System.out.println(entries);
                    CanalTest.parseEntry(entries);
                }
                //connector.ack(batchId);
            }
        } catch (CanalClientException e) {
            e.printStackTrace();
        }finally {
            connector.disconnect();
        }


    }

    private static void parseEntry(List<CanalEntry.Entry> entries){
            // 遍历消息

        for (CanalEntry.Entry entry:entries){

            //只处理ROWDATA数据
            if(entry.getEntryType()==CanalEntry.EntryType.TRANSACTIONBEGIN||
                    entry.getEntryType()==CanalEntry.EntryType.TRANSACTIONEND){
                continue;
            }

            //定义解析类
            CanalEntry.RowChange rowChange = null;

            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(entry.toString(),e);
            }

           // System.out.println("=======" + rowChange);

            CanalEntry.EventType eventType = rowChange.getEventType();
            System.out.println("操作类型:" + eventType );

            for (CanalEntry.RowData rowData:rowChange.getRowDatasList()){
                 if(eventType == CanalEntry.EventType.DELETE){
                     List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                     for (CanalEntry.Column column:beforeColumnsList){
                         System.out.println(column.getName() + "\t" + column.getValue() + "\t" + column.getUpdated());
                     }
                 }else if(eventType == CanalEntry.EventType.UPDATE){
                    List<CanalEntry.Column> beforeColumnsList = rowData.getBeforeColumnsList();
                    for (CanalEntry.Column column:beforeColumnsList){
                        System.out.println(column.getName() + "\t" + column.getValue() + "\t" + column.getUpdated());
                    }
                }
            }

        }

    }


}
