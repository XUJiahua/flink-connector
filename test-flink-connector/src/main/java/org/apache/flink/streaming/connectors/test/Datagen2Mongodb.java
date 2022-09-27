package org.apache.flink.streaming.connectors.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Created on 2021/9/14.
 *
 * @author MariaCarrie
 */
public class Datagen2Mongodb {

    public static void main(String args[]) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, settings);

        String pwd = System.getenv("MONGODB_PASSWORD");

        String url = "  'uri'='mongodb://evofinder:%s@hk1rbd-Testing:23636/?authSource=evofinder',\n";
        url = String.format(url, pwd);

        String sourceSql = "CREATE TABLE datagen (\n" +
                " k STRING,\n" +
                " hk STRING,\n" +
                " hv STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " 'fields.k.length'='1',\n" +
                " 'fields.hk.length'='1',\n" +
                " 'fields.hv.length'='1'\n" +
                ")";
        String sinkSql = "CREATE TABLE mongoddb (\n" +
                "  k STRING,\n" +
                "  hk STRING,\n" +
                "  hv STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'mongodb',\n" +
                "  'database'='evofinder',\n" +
                "  'collection'='flink_test',\n" +
                url +
                "  'maxConnectionIdleTime'='20000',\n" +
                "  'batchSize'='1'\n" +
                ")";
        String insertSql = "insert into mongoddb " +
                "select k,hk,hv " +
                "from datagen";

        tableEnvironment.executeSql(sourceSql);
        tableEnvironment.executeSql(sinkSql);
        tableEnvironment.executeSql(insertSql);
    }
}
