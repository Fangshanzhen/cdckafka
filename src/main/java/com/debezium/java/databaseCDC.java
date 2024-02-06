package com.debezium.java;

import com.alibaba.fastjson.JSONObject;
import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.debezium.java.CDCUtils.*;

/**
 * 监听的数据写入kafka，需自行消费kafka得到cdc数据
 * 使用debezium1.9.7.Final版本，超过2.0以后需要java11
 * 关系型数据库使用
 * 参考文档：https://debezium.io/documentation/reference/nightly/connectors/
 */

@Slf4j
public class databaseCDC {

    public static void cdcData(String originalDatabaseType, String originalDbname, String originalSchema, String originalIp, String originalPort,
                               String originalUsername, String originalPassword,
                               String tableList, String kafkaServer, String topic, String offsetAddress, String databaseHistoryAddress, String serverId, String slotName) throws Exception {


        if (tableList != null) {
            String modified = transformString(tableList, originalSchema);
            //创建存放目录
            createFile(offsetAddress, databaseHistoryAddress);

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Configuration config = Configuration.create()
                    .with("connector.class", connectorClass(originalDatabaseType))
                    .with("database.hostname", originalIp)
                    .with("database.port", originalPort)
                    .with("database.user", originalUsername)
                    .with("database.password", originalPassword)
                    .with("database.dbname", originalDbname)
                    .with("database.server.name", "my-cdc-server-" + originalDatabaseType)
                    .with("table.include.list", modified)
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
                        .with("slot.name", slotName) // 逻辑复制槽名称, 不能超过max_replication_slots = 20
                        .with("plugin.name", "pgoutput") //postgresql 单独配置，必须是pgoutput或decoderbufs
                        .build();
            }
            if (originalDatabaseType.equals("mysql")) {
                config = config.edit()
                        .with("database.server.id", serverId)   //mysql的 serverid
                        .with("converters", "dateConverters")   //解决mysql字段中的时区问题，设置with("database.serverTimezone", "Asia/Shanghai")无效
                        .with("dateConverters.type", "com.debezium.java.MySqlDateTimeConverter")
                        .build();      //
            }

            ExecutorService executorService = Executors.newSingleThreadExecutor();
            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        // 使用ExecutorService来异步处理记录并获取JSONObject
                        Future<JSONObject> futureJsonObject = executorService.submit(() -> {
                            Struct structValue = (Struct) record.value();
                            return CDCUtils.transformData(structValue, originalDatabaseType);
                        });
                        // 等待Future任务完成并获取结果
                        try {
                            JSONObject jsonObject = futureJsonObject.get();
                            //todo 处理JSONObject，写进kafka进行处理
                            try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props)) {
                                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, tableList, String.valueOf(jsonObject));
                                kafkaProducer.send(producerRecord).get();
                                log.info("数据写入kafka成功！");
                            } catch (InterruptedException | ExecutionException e) {
                                e.printStackTrace();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    })
                    .build();

            // 启动 engine
            engine.run();

        }
    }


}




