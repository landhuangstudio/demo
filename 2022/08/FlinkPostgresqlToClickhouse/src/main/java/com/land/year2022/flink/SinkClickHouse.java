package com.land.year2022.flink;


import com.google.gson.Gson;
import com.google.gson.internal.LinkedTreeMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;

public class SinkClickHouse extends RichSinkFunction<String> {
    Connection connection;

    PreparedStatement insertStmt;
    PreparedStatement deleteStmt;
    PreparedStatement updateStmt;

    private Connection getConnection() {
        Connection conn = null;
        try {
            Class.forName("cc.blynk.clickhouse.ClickHouseDriver");
            String url = "jdbc:clickhouse://127.0.0.1:8123/temp";
            conn = DriverManager.getConnection(url, "default", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String insertSql = "insert into test_source(id,name,py_code,seq_no,description) values (?,?,?,?,?)";
        String deleteSql = "delete from test_source where id=?";
        deleteSql = "ALTER TABLE test_source DELETE WHERE id=?";
        String updateSql = "update test_source set name=? ,py_code=?,seq_no=?,description=? where id=?";
        insertStmt = connection.prepareStatement(insertSql);
        deleteStmt = connection.prepareStatement(deleteSql);
        updateStmt = connection.prepareStatement(updateSql);

    }

    // 每条记录插入时调用一次
    public void invoke(String value, Context context) throws Exception {
        Gson t = new Gson();
        HashMap<String, Object> hs = t.fromJson(value, HashMap.class);
        System.out.println("value is => " + value);
        LinkedTreeMap<String, Object> source = (LinkedTreeMap<String, Object>) hs.get("source");
        String database = (String) source.get("db");
        String table = (String) source.get("table");
        String op = (String) hs.get("op");

//        value is => {"before":null,"after":{"id":1,"name":"2","py_code":"3","seq_no":4,"description":"5"},"source":{"version":"1.5.4.Final","connector":"postgresql","name":"postgres_cdc_source","ts_ms":1660700474903,"snapshot":"false","db":"temp_flink_cdc","sequence":"[null,\"2072754159272\"]","schema":"public","table":"test_source","txId":22671122,"lsn":2072754159368,"xmin":null},"op":"c","ts_ms":1660700475811,"transaction":null}
//        value is => {"before":{"id":1,"name":"2","py_code":"3","seq_no":4,"description":"5"},"after":{"id":1,"name":"2","py_code":"3","seq_no":4,"description":"55"},"source":{"version":"1.5.4.Final","connector":"postgresql","name":"postgres_cdc_source","ts_ms":1660701967985,"snapshot":"false","db":"temp_flink_cdc","sequence":"[\"2072754159552\",\"2072754159552\"]","schema":"public","table":"test_source","txId":22671125,"lsn":2072754188272,"xmin":null},"op":"u","ts_ms":1660701967654,"transaction":null}
//        value is => {"before":{"id":1,"name":"2","py_code":"3","seq_no":4,"description":"55"},"after":null,"source":{"version":"1.5.4.Final","connector":"postgresql","name":"postgres_cdc_source","ts_ms":1660701971752,"snapshot":"false","db":"temp_flink_cdc","sequence":"[\"2072754190088\",\"2072754190088\"]","schema":"public","table":"test_source","txId":22671126,"lsn":2072754190144,"xmin":null},"op":"d","ts_ms":1660701971218,"transaction":null}


        //实现insert方法
        if ("temp_flink_cdc".equals(database) && "test_source".equals(table)) {
            if ("r".equals(op) || "c".equals(op)) {
                LinkedTreeMap<String, Object> data = (LinkedTreeMap<String, Object>) hs.get("after");
                Double ids = (Double) data.get("id");
                int id = ids.intValue();
                String name = (String) data.get("name");
                String pyCode = (String) data.get("py_code");
                Double seqNo = (Double) data.get("seq_no");
                String description = (String) data.get("description");

                insertStmt.setInt(1, id);
                insertStmt.setString(2, name);
                insertStmt.setString(3, pyCode);
                insertStmt.setDouble(4, seqNo);
                insertStmt.setString(5, description);
                insertStmt.executeUpdate();
            } else if ("u".equals(op)) {
                LinkedTreeMap<String, Object> data = (LinkedTreeMap<String, Object>) hs.get("after");
                Double ids = (Double) data.get("id");
                int id = ids.intValue();
                String name = (String) data.get("name");
                String pyCode = (String) data.get("py_code");
                Double seqNo = (Double) data.get("seq_no");
                String description = (String) data.get("description");

                deleteStmt.setInt(1, id);
                deleteStmt.executeUpdate();
                //clickhouse update 这里使用先删除，后插入
                insertStmt.setInt(1, id);
                insertStmt.setString(2, name);
                insertStmt.setString(3, pyCode);
                insertStmt.setDouble(4, seqNo);
                insertStmt.setString(5, description);
                insertStmt.executeUpdate();
            } else if ("d".equals(op)) {
                LinkedTreeMap<String, Object> data = (LinkedTreeMap<String, Object>) hs.get("before");
                Double ids = (Double) data.get("id");
                int id = ids.intValue();
                deleteStmt.setInt(1, id);
                deleteStmt.executeUpdate();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (insertStmt != null) {
            insertStmt.close();
        }
        if (deleteStmt != null) {
            deleteStmt.close();
        }
        if (updateStmt != null) {
            updateStmt.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}