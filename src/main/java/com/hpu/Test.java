package com.hpu;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyn
 * @version 1.0
 * @date 2022/4/9 16:58
 */
public class Test {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        //TODO 1.1 开启CheckPoint
//        env.enableCheckpointing(5000L);
//        //env.getCheckpointConfig().setCheckpointStorage("hdfs://192.168.9.102:8020/211027/ck");
//        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//
//        env.setStateBackend(new FsStateBackend("hdfs://192.168.9.102:8020/211027/ck"));
//
//        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.创建Source对象
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop112")
                .port(3306)
                .username("root")
                .password("111111")
                .databaseList("gmall2021")
                .tableList("gmall2021.base_category1") //需要加上库名
                .deserializer(new JsonDebeziumDeserializationSchema())
                //earliest:需要在建库之前就开启Binlog,也就是说Binlog中需要有建库建表语句
                .startupOptions(StartupOptions.initial())
                .build();

        //TODO 3.读取数据
        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        //TODO 4.打印数据
        dataStreamSource.print();

        //TODO 5.启动
        env.execute();

    }
}
