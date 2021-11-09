package com.cn.it.warehouse.canal.canal2kafka;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.cn.it.warehouse.canal.utils.CanalConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Properties;

public class ClusterCanalClient extends AbstractCanalClient{


    private static String kafkaConfigPath = "kafka-server-config.properties";

    public ClusterCanalClient(String client_id) {
        super(client_id);
    }

    public ClusterCanalClient(String client_id, CanalConnector connector) {
        super(client_id, connector);
    }

    //创建kafka生产者
    public KafkaProducer createProducer(){
        //加载kafka配置文件
        Properties props = new Properties();

        try {
            props.load(new FileInputStream(ClusterCanalClient.class.getClassLoader().getResource(kafkaConfigPath).getPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new KafkaProducer<String,String>(props);
    }


    //往kafka推送cancal数据
    public static void main(String[] args) {

        ClusterCanalClient canalClient = new ClusterCanalClient(CanalConfigUtil.client_id);
        //创建canal连接
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop-101", 11111),
                "shop", "root", "_Qq3pw34w9bqa");
        //创建kafka客户端
        KafkaProducer producer = canalClient.createProducer();

        canalClient.setConnector(connector,producer, CanalConfigUtil.kafka_topic);

        canalClient.start();

        Runtime.getRuntime().addShutdownHook(new Thread(){
            @Override
            public void run() {
                try {
                    canalClient.stop();
                } catch (Exception e) {
                    e.printStackTrace();
                }finally {
                    if(producer!=null){
                        producer.close();
                    }
                }
            }
        });
    }
    @Override
    protected void start() {
        super.start();
    }

    @Override
    protected void stop() {
        super.stop();
    }

    @Override
    protected void process() {
        super.process();
    }

    @Override
    protected void processEntry(List<CanalEntry.Entry> entrys) {
        super.processEntry(entrys);
    }

    @Override
    protected String buildPositionForDump(CanalEntry.Entry entry) {
        return super.buildPositionForDump(entry);
    }

    @Override
    public void setConnector(CanalConnector connector) {
        super.setConnector(connector);
    }

    @Override
    public void setConnector(CanalConnector connector, KafkaProducer kafkaProducer, String kafkaTopic) {
        super.setConnector(connector, kafkaProducer, kafkaTopic);
    }
}
