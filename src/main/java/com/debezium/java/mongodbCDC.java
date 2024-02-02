package com.debezium.java;


import io.debezium.config.Configuration;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.history.FileDatabaseHistory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.json.JSONObject;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.debezium.java.CDCUtils.*;

/**
 * mongodb配置有所不同，单独区别
 */
@Slf4j
public class mongodbCDC {

    public static void cdcData(String originalDatabaseType, String host, String originalSchema, String tableList,
                               String originalUsername, String originalPassword,
                               String kafkaServer, String topic, String offsetAddress, String databaseHistoryAddress) throws Exception {


        if (tableList != null) {
            String modified = transformString(tableList, originalSchema);

            createFile(offsetAddress,databaseHistoryAddress);

            Properties props = new Properties();
            props.put("bootstrap.servers", kafkaServer);
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


            // 配置Debezium连接MongoDB的相关参数
            Configuration config = Configuration
                    .create()
                    .with("name", "my-mongodb-cdc")
                    .with("connector.class", "io.debezium.connector.mongodb.MongoDbConnector")
                    .with("mongodb.hosts", host)
                    .with("mongodb.name", "test")
//                    .with("mongodb.user", originalUsername)
//                    .with("mongodb.password", originalPassword)
                    .with("database.whitelist", originalSchema)
                    .with("collection.whitelist", modified)
                    .with("offset.storage", FileOffsetBackingStore.class.getName())
                    .with("offset.storage.file.filename", offsetAddress)
                    .with("offset.flush.interval.ms", 1000)
                    .with("database.history", FileDatabaseHistory.class.getName())
                    .with("database.history.file.filename", databaseHistoryAddress)
                    .with("snapshot.mode", "initial")

                    .build();


            ExecutorService executorService = Executors.newSingleThreadExecutor();
            EmbeddedEngine engine = EmbeddedEngine.create()
                    .using(config)
                    .notifying(record -> {
                        // 使用ExecutorService来异步处理记录并获取JSONObject
                        Future<JSONObject> futureJsonObject = executorService.submit(() -> {
                            Struct structValue = (Struct) record.value();
                            return mongodbUtils.structToJson(structValue);
                        });
                        try {
                            JSONObject jsonObject = futureJsonObject.get();
                            System.out.println(jsonObject);
                            //todo 处理JSONObject

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
