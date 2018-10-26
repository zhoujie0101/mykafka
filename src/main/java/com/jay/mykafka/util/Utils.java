package com.jay.mykafka.util;

import com.jay.mykafka.cluster.TopicPartition;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * jie.zhou
 * 2018/10/25 09:24
 */
public class Utils {
    public static Properties loadProps(String filename) throws IOException {
        InputStream in = Files.newInputStream(Paths.get(filename));
        Properties props = new Properties();
        props.load(in);

        return props;
    }

    public static String getString(Properties props, String key) {
        return getString(props, key, null);
    }

    public static String getString(Properties props, String key, String def) {
        if (props.containsKey(key)) {
            return props.getProperty(key);
        } else {
            return def;
        }
    }

    public static int getInt(Properties props, String key) {
        if (props.containsKey(key)) {
            return getInt(props, key, -1);
        } else {
            throw new IllegalArgumentException("Missing required property '" + key + "'");
        }
    }

    public static int getInt(Properties props, String key, int def) {
        return getIntInRange(props, key, def, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public static int getIntInRange(Properties props, String key, int def, int min, int max) {
        int val;
        if (props.containsKey(key)) {
            val = Integer.parseInt(props.getProperty(key));
        } else {
            val = def;
        }
        if (val < min || val > max) {
            throw new IllegalArgumentException(key + " has value " + val + " which is not in the range ("
                    + min + ", " + max + ")");
        }

        return val;
    }

    public static Map<String, Integer> getTopicFileSize(String logFileSizes) {
        return getCSVMap(logFileSizes);
    }


    public static Map<String, Integer> getTopicRollHours(String rollHours) {
        return getCSVMap(rollHours);
    }

    private static Map<String, Integer> getCSVMap(String csvVals) {
        Map<String, Integer> map = new HashMap<>();
        if ("".equals(csvVals)) {
            return map;
        }
        String[] vals = csvVals.split(",");
        for (String v : vals) {
            String[] temp = v.split(":");
            map.put(temp[0], Integer.valueOf(temp[1]));
        }

        return map;
    }

    public static Map<String, Integer> getTopicRetentionHours(String retentionHours) {
        return getCSVMap(retentionHours);
    }

    public static Map<String, Integer> getTopicRetentionSize(String retentionSize) {
        return getCSVMap(retentionSize);
    }

    public static boolean getBoolean(Properties props, String key, boolean def) {
        if (!props.containsKey(key)) {
            return def;
        } else if ("true".equals(props.getProperty(key))) {
            return true;
        } else if ("false".equals(props.getProperty("key"))) {
            return false;
        } else {
            throw new IllegalArgumentException("Unacceptable value for property '" + key
                    + "', boolean values must be either 'true' or 'false'");
        }
    }

    public static long getLong(Properties props, String key) {
        if (props.containsKey(key)) {
            return getLong(props, key, -1);
        } else {
            throw new IllegalArgumentException("Missing required property '" + key + "'");
        }
    }

    public static long getLong(Properties props, String key, long def) {
        return getLongInRange(props, key, def, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    private static long getLongInRange(Properties props, String key, long def, long min, long max) {
        long val;
        if (props.containsKey(key)) {
            val = Long.parseLong(props.getProperty(key));
        } else {
            val = def;
        }
        if (val < min || val > max) {
            throw new IllegalArgumentException(key + " has value " + val + " which is not in the range ("
                    + min + ", " + max + ")");
        }

        return val;
    }

    public static Map<String, Integer> getTopicFlushIntervals(String flushIntervals) {
        return getCSVMap(flushIntervals);
    }

    public static Map<String, Integer> getTopicPartitions(String partitions) {
        return getCSVMap(partitions);
    }

    public static TopicPartition getTopicPartition(String topicPartition) {
        int index = topicPartition.lastIndexOf("-");
        return new TopicPartition(topicPartition.substring(0, index), topicPartition.substring(index + 1));
    }

    public static Thread newThread(String name, Runnable r, boolean daemon) {
        Thread thread = new Thread(r, name);
        thread.setDaemon(daemon);
        return thread;
    }
}
