package com.hpu;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zyn
 * @version 1.0
 * @date 2022/4/9 15:59
 */
public class FlinkCDC_DataStream {
    public static void main(String[] args) throws Exception {
        //TODO 1.初始化流的执行环境

        Configuration configuration = new Configuration();

        //https://www.cnblogs.com/createweb/p/12027737.html
        configuration.setLong("akka.ask.timeout", 60000);
        configuration.setLong("web.timeout", 60000);


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);


        //TODO 2.开启检查点 FlinkCDC将读取binlog的位置信息以状态的方式保存在CK，实现断点续传
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointStorage("hdfs://zyn-node01:8020/FlinkCDC/checkpoints");
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);

        env.setStateBackend(new HashMapStateBackend());
//        env.setStateBackend(new FsStateBackend("hdfs://zyn-node01:8020/FlinkCDC/statebackend"));

        System.setProperty("HADOOP_USER_NAME", "hadoop");


        //TODO 3.创建FlinkMysqlCDC的Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("zyn-node01")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall")
                .tableList("gmall.stu3")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();

        //TODO 4.使用CDC Source从MYSQL读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(),
                "MysqlSource");

        mysqlDS.print();

        env.execute();


    }
}
