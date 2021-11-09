package com.cn.it.warehouse.canal.utils;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class CanalConfigUtil {

    private static Config config = ConfigFactory.load("canal_client.properties");

    public static String zookeeper_host = null;
    public static String canal_destinations = null;
    public static String client_id = null;
    public static String kafka_topic = null;
    public static String broker_host = null;


    static {
        zookeeper_host = config.getString("zookeeper.host");
        canal_destinations = config.getString("canal.destinations");
        client_id = config.getString("client.id");
        kafka_topic = config.getString("kafka.topic");
        broker_host = config.getString("broker.host");
    }

    public static void main(String[] args) {
        System.out.println(CanalConfigUtil.zookeeper_host);
    }

}
