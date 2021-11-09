package com.cn.it.warehouse.canal.canal2kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * 基类
 *
 * 源码来自 alibaba
 */
public class AbstractCanalClient {

    protected final static Logger             debugLogger        = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected final static Logger             logger             = LoggerFactory.getLogger(AbstractCanalClient.class);
    protected static final String             DATE_FORMAT        = "yyyy-MM-dd HH:mm:ss";
    protected volatile boolean                running            = false;
    protected Thread                          thread             = null;
    protected CanalConnector                  connector;
    protected static String                   context_format     = null;
    protected String                          client_id;
    protected String                          kafkaTopic         = null;
    protected KafkaProducer                   kafkaProducer      = null;

    private long messageLentth = 0;


    static {
        context_format += "* * *  Batch Id: [{}] ,count : [{}] , memsize : [{}] , Time : {},Start : [{}] End : [{}]   * * * ";
    }

    public AbstractCanalClient(String client_id){
        this(client_id, null);
    }

    public AbstractCanalClient(String client_id, CanalConnector connector){
        this.client_id = client_id;
        this.connector = connector;
    }

    // 启动 服务
    protected void start() {
        //判断参数connector是否为空
        Assert.notNull(connector, "连接为空");
        thread = new Thread(new Runnable() {
            public void run() {
                process();
            }
        });
        //抛出线程里面的异常
        //thread.setUncaughtExceptionHandler(handler);
        thread.start();
        running = true;
    }

    protected void stop() {
        if (!running) {
            return;
        }
        running = false;
        if (thread != null) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                // ignore
            }
        }
        MDC.remove("client_id");
    }

    protected void process() {
        int batchSize = 5 * 1024;
        // client 死循环，拉取数据
        while (running) {
            try {
                connector.connect();
                connector.subscribe();
                while (running) {
                    Message message = connector.getWithoutAck(batchSize); // 获取指定数量的数据
//                  connector.get();
                    long batchId = message.getId();
                    int size = message.getEntries().size();

                    logger.debug("pull message batchId {} and size {} and start process",batchId,size);

                    if (batchId != -1 && size != 0) {
                        processEntry(message.getEntries());    // 处理数据
                        printSummary(message,batchId,batchSize);    // 打印处理日志
                    }

                    logger.debug("ack message batchId {} ",batchId);
                    connector.ack(batchId); // 提交确认
                    // connector.rollback(batchId); // 处理失败, 回滚数据
                    // Thread.sleep(10000L);

                    logger.debug("pull message batchId {} and size {}",batchId,size);
                }
                logger.debug("当前状态 {}",running);

            } catch (Exception e) {
                logger.error("process error!", e);
            } finally {
                connector.disconnect();
                MDC.remove("client_id");
            }
        }
    }

    protected void processEntry(List<Entry> entrys) {

        long aa = 0;
        logger.debug("the entrys start size is {} ",entrys.size());
        for (Entry entry : entrys) {
            aa ++;
            long executeTime = entry.getHeader().getExecuteTime();
            long delayTime = new Date().getTime() - executeTime;

            if (entry.getEntryType() == EntryType.ROWDATA) {
                RowChange rowChage = null;
                try {
                    rowChage = RowChange.parseFrom(entry.getStoreValue());
                } catch (Exception e) {
                    throw new RuntimeException("parse event has an error , data:" + entry.toString(), e);
                }

                logger.debug("process entrys is {} and  entry.getEntryType ",aa );

                EventType eventType = rowChage.getEventType();

                if (eventType == EventType.QUERY) {
                    logger.info(" sql ----> " + rowChage.getSql() );
                    continue;
                } else if(rowChage.getIsDdl()){
                    logger.debug("process entrys is {} and  entry.getIsDdl start ",aa );

                    logger.debug("process entrys is {} and  entry.getIsDdl end ",aa );

                }else {
                    parseAndFormat(entry);
                }
            }

            logger.debug("process entrys is {}  ",aa );
        }


        logger.debug("process entrys end is {} ",aa);
    }

    // 数据format 操作
    private void parseAndFormat(Entry entry) {

        logger.debug("parseAndFormat  start");

        RowChange rowChage = null;

        JSONArray colsJson = null;
        JSONObject rowJson = new JSONObject();

        StringBuffer binlog = new StringBuffer(entry.getHeader().getLogfileName());
        binlog.append(".");
        binlog.append(entry.getHeader().getLogfileOffset());

        StringBuffer binlogkey = null;

        String tablename = entry.getHeader().getSchemaName() + "." + entry.getHeader().getTableName();


        // binlog 属性
        try {
            rowChage = RowChange.parseFrom(entry.getStoreValue());
            rowJson.put("binlog",binlog.toString());
            rowJson.put("table",tablename);
            rowJson.put("exe_time",String.valueOf(entry.getHeader().getExecuteTime()));
            rowJson.put("cur_time",String.valueOf(new Date().getTime()));
            rowJson.put("event_type", rowChage.getEventType());

            /**
             * 以行为单位处理数据,即使数据变更时是批处理
             */

           long binlogsize = 0;

            if(rowChage.getEventType()==EventType.INSERT) {

                logger.info("the number of data list is  {} ",rowChage.getRowDatasList().size());

                for (RowData rowData : rowChage.getRowDatasList()) {

                    binlogkey = new StringBuffer(binlog.toString());
                    binlogkey.append(".");
                    binlogkey.append(binlogsize);
                    binlogsize++;

                    colsJson = getColsJson(rowData.getAfterColumnsList());
                    rowJson.remove("cols");  // 批处理时会有多条数据,首先删除掉原来的数据,然后再加入新数据
                    rowJson.put("cols", colsJson);
                    logger.info("this is INSERT  binlog key {} table {} messageLentth {} ",binlogkey,tablename,messageLentth++);
                    debugLogger.debug("this is INSERT option and binlog key {}   and  message   {}",binlogkey,rowJson.toString());

                    logger.debug("kafka send  INSERT start ");
                    logger.info(rowJson.toString());
                    this.kafkaProducer.send(new ProducerRecord<String,String>(this.kafkaTopic,binlogkey.toString(),rowJson.toString()));
                    logger.debug("kafka send  INSERT end ");
                }
            }else if(rowChage.getEventType()==EventType.DELETE){

                for (RowData rowData : rowChage.getRowDatasList()) {

                    binlogkey = new StringBuffer(binlog.toString());
                    binlogkey.append(".");
                    binlogkey.append(binlogsize);

                    binlogsize++;

                    colsJson = getColsJson(rowData.getBeforeColumnsList());
                    rowJson.remove("cols");  // 批处理时会有多条数据,首先删除掉原来的数据,然后再加入新数据
                    rowJson.put("cols", colsJson);
                    logger.info("this is DELETE binlog key {} table {} messageLentth {}",binlogkey,tablename,messageLentth++);
                    debugLogger.debug("this is DELETE option and binlog key {}   and  message   {}",binlogkey,rowJson.toString());

                    logger.debug("kafka send  DELETE start ");
                    logger.info(rowJson.toString());
                    this.kafkaProducer.send(new ProducerRecord<String,String>(this.kafkaTopic,binlogkey.toString(),rowJson.toString()));
                    logger.debug("kafka send DELETE end ");
                }
            }else if(rowChage.getEventType()==EventType.UPDATE){
                for (RowData rowData : rowChage.getRowDatasList()) {

                    binlogkey = new StringBuffer(binlog.toString());
                    binlogkey.append(".");
                    binlogkey.append(binlogsize);

                    binlogsize++;

                    colsJson = getColsJson(rowData.getBeforeColumnsList(),rowData.getAfterColumnsList());
                    rowJson.remove("cols");  // 批处理时会有多条数据,首先删除掉原来的数据,然后再加入新数据
                    rowJson.put("cols", colsJson);
                    logger.info("this is UPDATE binlog key {} table {} messageLentth {}",binlogkey,tablename,messageLentth++);
                    debugLogger.debug("this is UPDATE option and binlog key {}   and  message   {}",binlogkey,rowJson.toString());

                    logger.debug("kafka send UPDATE start ");
                    logger.info(rowJson.toString());
                    this.kafkaProducer.send(new ProducerRecord<String,String>(this.kafkaTopic,binlogkey.toString(),rowJson.toString()));

                    logger.debug("kafka send UPDATE end ");
                }
            }
        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     *  处理删除和插入时的数据
     * @param cols
     * @return
     */
    private JSONArray getColsJson(List<Column> cols){

        JSONArray colsJson = new JSONArray();
        JSONObject colJson = null;
        for (Column column : cols) {
            colJson = new JSONObject();
            colJson.put("col", column.getName());
            colJson.put("val", column.getValue());
            colJson.put("type", column.getMysqlType());
            colsJson.add(colJson);
        }

        return colsJson;

    }

    /**
     *
     * @param beforeCols
     * @param afterCols
     * @return
     *
     * 正常情况下,修改前和修改后的值是一一对应的,而且发生修改的列并不会太多
     * 思路:
     * 1.首先判断该列是否发生改变,如果不改变,则只引用after值
     * 2.列值发生改变,则依据after位置去找before,判断位置是否相同
     * 3.如果位置相同,则直接将before值按照需要传送过去,如果不同,则循环整个row,去寻找与after相同的列
     *
     */
    private JSONArray getColsJson(List<Column> beforeCols,List<Column> afterCols){

        JSONArray colsJson = new JSONArray();
        JSONObject colJson = null;

        Column afterCol = null;
        Column beforeCol = null;

        for(int i=0;i<afterCols.size();i++){
            afterCol = afterCols.get(i);

            colJson = new JSONObject();
            colJson.put("col", afterCol.getName());
            colJson.put("val", afterCol.getValue());
            colJson.put("type", afterCol.getMysqlType());

            if(afterCol.getUpdated()){
                //
                if(i<beforeCols.size())
                    beforeCol = beforeCols.get(i);

                // 判断是否需要再次循环,一般是只有在极端的情况下才会去判断.也就是修改前和修改后的值不能一一对应的情况下,才会去再次判断
                // NOOP 调度算法读取数据
                if(i>=beforeCols.size() || !beforeCol.getName().equals(afterCol.getName())){
                    debugLogger.info("this getBeforeColumnsList and getAfterColumnsList has different queue. so this execute for loop and beforecol:{} afterCol:{}",i,afterCol.getName());
                    for(Column col:beforeCols){
                        if(col.getName().equals(afterCol.getName())){
                            beforeCol = col;
                            break;
                        }
                    }
                }
                colJson.put("old_val", beforeCol.getValue());
                colJson.put("updated", afterCol.getUpdated());
            }

            colsJson.add(colJson);
        }
        return colsJson;
    }

    private void printSummary(Message message, long batchId, int size) {
        long memsize = 0;
        for (Entry entry : message.getEntries()) {
            memsize += entry.getHeader().getEventLength();
        }

        String startPosition = null;
        String endPosition = null;
        if (!CollectionUtils.isEmpty(message.getEntries())) {
            startPosition = buildPositionForDump(message.getEntries().get(0));
            endPosition = buildPositionForDump(message.getEntries().get(message.getEntries().size() - 1));
        }

        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        logger.info(context_format, new Object[] { batchId, size, memsize, format.format(new Date()), startPosition,
                endPosition });
    }

    protected String buildPositionForDump(Entry entry) {
        long time = entry.getHeader().getExecuteTime();
        Date date = new Date(time);
        SimpleDateFormat format = new SimpleDateFormat(DATE_FORMAT);
        return entry.getHeader().getLogfileName() + ":" + entry.getHeader().getLogfileOffset() + ":"
                + entry.getHeader().getExecuteTime() + "(" + format.format(date) + ")";
    }

    public void setConnector(CanalConnector connector) {
        this.connector = connector;
    }

    public void setConnector(CanalConnector connector, KafkaProducer kafkaProducer, String kafkaTopic) {
        this.connector = connector;
        this.kafkaProducer = kafkaProducer;
        this.kafkaTopic = kafkaTopic;
    }
}