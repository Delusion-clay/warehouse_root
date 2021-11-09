package com.cn.it.warehouse.realtime.utils;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scala.collection.immutable.Map$;

import java.util.List;
import java.util.stream.Collectors;


/**
 * @description:  map转MAP
 */
public class MaptranUtil {


    public static <K,V> scala.collection.immutable.Map<K,V> toScalaImmutableMap(java.util.Map<K,V> jmap){

        List<Tuple2<K, V>> tuples = jmap.entrySet().stream()
                .map(e -> Tuple2.apply(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        Seq<Tuple2<K, V>> tuple2Seq = JavaConverters.asScalaBufferConverter(tuples).asScala().toSeq();

        //在java里面怎么调用scala方法
       return (scala.collection.immutable.Map<K,V>)Map$.MODULE$.apply(tuple2Seq);
    }
}
