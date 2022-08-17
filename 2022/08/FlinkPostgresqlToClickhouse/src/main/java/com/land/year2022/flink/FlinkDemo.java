package com.land.year2022.flink;

import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Properties;


/**
 * @author  ken
 */
public class FlinkDemo {

    public static void main(String[] args) throws Exception {
        System.out.println("start flink pg to ck....");
        sourceForPostgreSql();
        System.out.println("end flink pg to ck....");
    }


    public static void sourceForPostgreSql() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("snapshot.mode", "always");
        //debezium 小数转换处理策略
        properties.setProperty("decimal.handling.mode", "double");
        //debezium 配置以database. 开头的属性将被传递给jdbc url
        properties.setProperty("database.serverTimezone", "GMT+8");

        SourceFunction<String> sourceFunction = PostgreSQLSource.<String>builder()
                .hostname("127.0.0.0=1")
                .port(5432)
                // monitor postgres database
                .database("temp_flink_cdc")
                // monitor inventory schema
                .schemaList("public")
                // monitor products table
                .tableList("public.test_source")
                .username("postgres")
                .password("123456")
                // pg解码插件
                .decodingPluginName("pgoutput")
                // 复制槽名称 不能重复
                .slotName("test_source_slot2")
                // converts SourceRecord to JSON String
                .deserializer(new JsonDebeziumDeserializationSchema())
                .debeziumProperties(properties)
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env
                .addSource(sourceFunction)
                .addSink(new SinkClickHouse())
//                .print()
                // use parallelism 1 for sink to keep message ordering 对接收器使用并行度1来保持消息顺序
                .setParallelism(1);
        env.execute();
    }
}
