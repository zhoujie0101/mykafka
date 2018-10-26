package com.jay.mykafka.util;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * jie.zhou
 * 2018/10/25 15:46
 */
public class ZKUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtils.class);

    public static final String CONSUMERS_PATH = "/consumers";
    public static final String BROKER_IDS_PATH = "/brokers/ids";
    public static final String BROKER_TOPICS_PATH = "/brokers/topics";

    public static void makeSurePersistentPathExists(ZkClient client, String path) {
        if (!client.exists(path)) {
            client.createPersistent(path, true);
        }
    }

    public static void createEphemeralPathExpectConflict(ZkClient client, String path, String data) {
        try {
            createEphemeralPath(client, path, data);
        } catch (ZkNodeExistsException e) {
            // this can happen when there is connection loss; make sure the data is what we intend to write
            try {
                String storedData = client.readData(path);
                if (storedData == null || !storedData.equals(data)) {
                    LOGGER.info("conflict in " + path + " data: " + data + " stored data: " + storedData);
                    throw e;
                } else {
                    LOGGER.info(path + " exists with value " + data + " during connection loss; this is ok");
                }
            } catch (ZkNoNodeException e1) {
                // the node disappeared; treat as if node existed and let caller handles this
            } catch (Exception e2) {
                throw e2;
            }
        } catch (Exception e2) {
            throw e2;
        }
    }

    private static void createEphemeralPath(ZkClient client, String path, String data) {
        try {
            client.createEphemeral(path, data);
        } catch (ZkNoNodeException e) {
            createParentPath(client, path);
            client.createEphemeral(path, data);
        }
    }

    private static void createParentPath(ZkClient client, String path) {
        String parent = path.substring(0, path.lastIndexOf("/"));
        if (parent.length() != 0) {
            client.createPersistent(parent, true);
        }
    }
}
