package org.example.flink.source;

import ch.qos.logback.classic.LoggerContext;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.LoggerFactory;

public class KafkaSourceConnectorJson {
    public static void main(String[] args) throws Exception {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        ch.qos.logback.classic.Logger rootLogger = loggerContext.getLogger("org.apache.flink");
        rootLogger.setLevel(ch.qos.logback.classic.Level.INFO);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("TARGET_TOPIC")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliverGuarantee(DeliveryGuarantee.NONE)
                .build();





//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder().setBootstrapServers("localhost:9092")
                .setGroupId("flink-consumer-group").setTopics("SOURCE_TOPIC").setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("commit.offsets.on.checkpoint", "true")
                .setProperty("auto.commit.interval.ms", "3000")
                .setProperty("enable.auto.commit", "true").build();


//        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
//                .map(value -> {
//                    System.out.println("Forwarding to sink: " + value);
//                    return value;
//                })
//                .sinkTo(kafkaSink);

//        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
//                .map(msg -> {
//                    System.out.println("IN: " + msg);
//                    return msg;
//                })
//                .sinkTo(kafkaSink);


        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source").sinkTo(kafkaSink);



        env.execute("Kafka Source Connector");
    }
}
