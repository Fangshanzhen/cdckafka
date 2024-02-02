package com.debezium.java;

import org.apache.kafka.connect.data.Struct;
import org.json.JSONObject;
import java.text.SimpleDateFormat;
import java.util.Date;

public class mongodbUtils {

    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static JSONObject structToJson(Struct struct) {
        JSONObject jsonResult = new JSONObject();

        if (String.valueOf(struct).contains("source")) {
            Struct sourceStruct = struct.getStruct("source");
            String database = sourceStruct.getString("db");
            String table = sourceStruct.getString("collection");
            long tsMs = sourceStruct.getInt64("ts_ms");

            String formattedDate = sdf.format(new Date(tsMs));

            jsonResult.put("database", database);
            jsonResult.put("table", table);
            jsonResult.put("operate_ms", formattedDate);
        }

        // 获取操作类型
        if (String.valueOf(struct).contains("op")) {
            String opType = struct.getString("op");

            switch (opType) {
                case "c": // Create
                    jsonResult.put("operate_type", "create");
                    String afterStructCreate = String.valueOf(struct.get("after"));
                    JSONObject afterCreate = new JSONObject(afterStructCreate);
                    jsonResult.put("afterJson", afterCreate);
                    break;

                case "u": // Update
                    jsonResult.put("operate_type", "update");
                    String patchStructUpdate = String.valueOf(struct.get("patch"));
                    JSONObject patchUpdate = new JSONObject(patchStructUpdate);
                    String filterStructUpdate = String.valueOf(struct.get("filter"));
                    JSONObject filterUpdate = new JSONObject(filterStructUpdate);
                    jsonResult.put("beforeJson", filterUpdate);
                    jsonResult.put("afterJson", patchUpdate.getJSONObject("$set"));
                    break;

                case "d": // Delete
                    jsonResult.put("operate_type", "delete");
                    String filterStructDelete = String.valueOf(struct.get("filter"));
                    JSONObject filterDelete = new JSONObject(filterStructDelete);
                    jsonResult.put("beforeJson", filterDelete);
                    break;

                case "r": // Read
                    jsonResult.put("operate_type", "read");
                    String afterStructRead = String.valueOf(struct.get("after"));
                    JSONObject afterReade = new JSONObject(afterStructRead);
                    jsonResult.put("afterJson", afterReade);
                    break;
            }
        }
        return jsonResult;
    }


}
