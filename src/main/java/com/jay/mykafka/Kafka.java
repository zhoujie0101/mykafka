package com.jay.mykafka;

import com.jay.mykafka.conf.KafkaConfig;
import com.jay.mykafka.server.KafkaServer;
import com.jay.mykafka.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/25 09:17
 */
public class Kafka {
    private static final Logger LOGGER = LoggerFactory.getLogger(Kafka.class);

    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("USAGE: java [options] " + Kafka.class.getSimpleName() + " server.properties");
            System.exit(1);
        }

        try {
            Properties props = Utils.loadProps(args[0]);
            KafkaConfig conf = new KafkaConfig(props);

            KafkaServer server = new KafkaServer(conf);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                server.shutdown();
                server.awaitShutdown();
            }));

            server.startup();
            server.awaitShutdown();
        } catch (Exception e) {
            LOGGER.error("kafka start error", e);
        }

        System.exit(0);
    }
}
