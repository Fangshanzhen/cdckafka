package com.debezium.java;

import com.alibaba.fastjson.JSONObject;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import static com.debezium.java.CDCUtils.*;

/**
 * 监听数据放入队列中
 */


@Slf4j
public class cdc2queue {


    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String offsetAddress, String databaseHistoryAddress, String serverId, String slotName) throws Exception {

        if (tableList != null) {
            String modified = transformString(tableList, originalSchema);
            //创建存放目录
            createFile(offsetAddress, databaseHistoryAddress);

            Configuration config = Configuration.create()
                    .with("connector.class", connectorClass(originalDatabaseType))
                    .with("database.hostname", originalIp)
                    .with("database.port", originalPort)
                    .with("database.user", originalUsername)
                    .with("database.password", originalPassword)
                    .with("database.dbname", originalDbname)
                    .with("database.server.name", "my-cdc-server-" + originalDatabaseType)
                    .with("table.include.list", modified)
                    .with("database.schema", originalSchema)
                    .with("include.schema.changes", "false")
                    .with("name", "my-connector-" + originalDatabaseType)
                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                    .with("offset.storage.file.filename", offsetAddress)
                    .with("offset.flush.interval.ms", 2000)
                    .with("database.history", FileDatabaseHistory.class.getName())
                    .with("database.history.file.filename", databaseHistoryAddress)
                    .with("logger.level", "DEBUG")
                    .with("snapshot.mode", "initial") //全量+增量
                    .with("database.serverTimezone", "Asia/Shanghai")

                    .build();

            if (originalDatabaseType.equals("postgresql")) {
                config = config.edit()
                        .with("slot.name", slotName) // postgresql 单独配置， 逻辑复制槽名称, 不能超过max_replication_slots = 20
                        .with("plugin.name", "pgoutput")      //postgresql 单独配置，必须是pgoutput或decoderbufs
                        .build();
            }
            if (originalDatabaseType.equals("mysql")) {
                config = config.edit()
                        .with("database.server.id", serverId)   //mysql的 serverid
                        .with("converters", "dateConverters")   //解决mysql字段中的时区问题，某个字段如果是timestamp等类型，监控时debezium会转为utc，小8小时，设置with("database.serverTimezone", "Asia/Shanghai")无效
                        .with("dateConverters.type", "com.debezium.java.MySqlDateTimeConverter")
                        .build();
            }
            BlockingQueue queue = new LinkedBlockingDeque();

            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        Struct structValue = (Struct) record.value();
                        JSONObject operateJson = transformData(structValue, originalDatabaseType);
                        // 将转换后的JSON对象放入队列，等待被下一个节点消费
                        queue.offer(operateJson);
                    })
                    .build();

            // 启动一个线程来运行Debezium Engine
            new Thread(() -> {
                engine.run();
            }).start();

//             启动OperateJsonProcessor来处理队列中的数据
            OperateJsonProcessor processor = new OperateJsonProcessor(queue);
            new Thread(processor).start();
        }

    }


}




