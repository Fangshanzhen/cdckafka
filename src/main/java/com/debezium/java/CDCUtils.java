package com.debezium.java;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Field;
import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;


@Slf4j
public class CDCUtils {


    public static JSONObject transformData(Struct structValue, String type) {
        JSONObject operateJson = new JSONObject();
        if ((String.valueOf(structValue).contains("after") || String.valueOf(structValue).contains("before"))) {
            Struct afterStruct = structValue.getStruct("after");
            Struct beforeStruct = structValue.getStruct("before");

            JSONObject beforeJson = new JSONObject();
            JSONObject afterJson = new JSONObject();

            //操作类型
            String operateType = null;

            List<Field> fieldsList = null;
            List<Field> beforeStructList = null;

            if (afterStruct != null && beforeStruct != null) {
                operateType = "update";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    afterJson.put(fieldName, fieldValue);
                }
                beforeStructList = beforeStruct.schema().fields();
                for (Field field : beforeStructList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    beforeJson.put(fieldName, fieldValue);  //字段后面加@
                }

            } else if (afterStruct != null) {
                operateType = "insert";
                fieldsList = afterStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = afterStruct.get(fieldName);
                    afterJson.put(fieldName, fieldValue);
                }
            } else if (beforeStruct != null) {
                operateType = "delete";
                fieldsList = beforeStruct.schema().fields();
                for (Field field : fieldsList) {
                    String fieldName = field.name();
                    Object fieldValue = beforeStruct.get(fieldName);
                    beforeJson.put(fieldName, fieldValue);
                }
            } else {
                log.info("-----------数据无变化-------------");
            }

            operateJson.put("beforeJson", beforeJson);
            operateJson.put("afterJson", afterJson);

            Struct source = structValue.getStruct("source");
            //操作的数据库名
            String database = source.getString("db");
            //操作的表名
            String table = source.getString("table");
            if (!type.equals("mysql")) {    //mysql没有schema
                String schema = source.getString("schema");
                operateJson.put("schema", schema);
            }

            long tsMs = source.getInt64("ts_ms");
            long tsMs1 = structValue.getInt64("ts_ms");
            if (tsMs > tsMs1) {   //不用type，改用时间比较更准确
                tsMs = tsMs - 8 * 60 * 60 * 1000;  //oracle、sqlserver时间会多8小时
            }
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            String date = sdf.format(new Date(tsMs));

            operateJson.put("database", database);
            operateJson.put("table", table);
            operateJson.put("operate_ms", date);
            operateJson.put("operate_type", operateType);

        }
        return operateJson;

    }

    public static String transformString(String original, String prefix) {
        // 分割原始字符串
        String[] elements = original.split(",");
        StringBuilder result = new StringBuilder();

        // 遍历所有元素，给每个元素加上前缀
        for (int i = 0; i < elements.length; i++) {
            result.append(prefix).append("."); // 加上前缀.
            result.append(elements[i]); // 加上原始元素
            if (i < elements.length - 1) {
                result.append(","); // 如果不是最后一个元素，则加上逗号
            }
        }

        return result.toString();
    }


    public static void createFile(String offsetAddress, String databaseHistoryAddress) {
        List<String> fileList = new ArrayList<>();
        fileList.add(offsetAddress);
        fileList.add(databaseHistoryAddress);
        try {
            for (String s : fileList) {
                File file = new File(s);
                if (file.createNewFile()) {
                    log.info("File created: " + file.getName());
                } else {
                    log.info("File already exists.");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static String connectorClass(String originalDatabaseType) {
        if (originalDatabaseType.toLowerCase().equals("postgresql")) {
            return "io.debezium.connector.postgresql.PostgresConnector";
        }
        if (originalDatabaseType.toLowerCase().equals("mysql")) {
            return "io.debezium.connector.mysql.MySqlConnector";
        }
        if (originalDatabaseType.toLowerCase().equals("oracle")) {
            return "io.debezium.connector.oracle.OracleConnector";
        }
        if (originalDatabaseType.toLowerCase().equals("sqlserver")) {
            return "io.debezium.connector.sqlserver.SqlServerConnector";
        }
        if (originalDatabaseType.toLowerCase().equals("db2")) {
            return "io.debezium.connector.db2.Db2Connector";
        }

        return null;
    }

}
