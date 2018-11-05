package com.jay.mykafka.network;

import com.jay.mykafka.server.KafkaRequestHandler;
import com.jay.mykafka.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * kafka socket服务
 * jie.zhou
 * 2018/10/25 11:19
 */
public class SocketServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SocketServer.class);

    private int port;
    private int numProcessorThreads;
    private int monitoringPeriodSecs;
    private int sendBufferSize;
    private int receiveBufferSize;
    private int maxRequestSize;
    private Acceptor acceptor;
    private Processor[] processors;
    private KafkaRequestHandler handler;

    public SocketServer(int port, int numProcessorThreads, int monitoringPeriodSecs, int sendBufferSize,
                        int receiveBufferSize, int maxRequestSize, KafkaRequestHandler handler) {
        this.port = port;
        this.numProcessorThreads = numProcessorThreads;
        this.monitoringPeriodSecs = monitoringPeriodSecs;
        this.sendBufferSize = sendBufferSize;
        this.receiveBufferSize = receiveBufferSize;
        this.maxRequestSize = maxRequestSize;
        this.handler = handler;

        processors = new Processor[numProcessorThreads];
        acceptor = new Acceptor(port, processors, sendBufferSize, receiveBufferSize);
    }

    public void startup() {
        for (int i = 0; i < numProcessorThreads; i++) {
            processors[i] = new Processor(maxRequestSize);
            Utils.newThread("kafka-processor-" + i, processors[i], false).start();
        }
        Utils.newThread("kafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
    }

    public void shutdown() {
        acceptor.shutdown();
        for (Processor processor : processors) {
            processor.shutdown();
        }
    }

    /**
     * kafka socket handle
     */
    private class Processor implements Runnable {
        private ConcurrentLinkedQueue<SocketChannel> newConnections = new ConcurrentLinkedQueue<>();
        private int maxRequestSize;
        private Selector selector;
        private CountDownLatch startupLatch = new CountDownLatch(1);
        private CountDownLatch shutdownLatch = new CountDownLatch(1);
        private AtomicBoolean running = new AtomicBoolean(true);

        public Processor(int maxRequestSize) {
            this.maxRequestSize = maxRequestSize;
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException("Processor Selector open error", e);
            }
        }

        @Override
        public void run() {
            startupLatch.countDown();
            while (running.get()) {
                try {
                    configureNewConnections();
                    int ready = selector.select(500);
                    if (ready > 0) {
                        Set<SelectionKey> keys = selector.selectedKeys();
                        Iterator<SelectionKey> it = keys.iterator();
                        while (it.hasNext() && running.get()) {
                            SelectionKey key = it.next();
                            it.remove();
                            if (key.isReadable()) {
                                read(key);
                            } else if (key.isWritable()) {
                                write(key);
                            } else if (!key.isValid()) {
                                close(key);
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                selector.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            shutdownLatch.countDown();
        }

        private void read(SelectionKey key) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Receive request = (Receive) key.attachment();
            if (request == null) {
                request = new BoundedByteBufferReceive(maxRequestSize);
                key.attach(request);
            }
            int read = request.readFrom(socketChannel);
            if (read < 0) {
                close(key);
            } else if (request.complete()){
                Send maybeResponse = handler.handleRequest(key, request);
                key.attach(null);
                if (maybeResponse != null) {
                    key.attach(maybeResponse);
                    key.interestOps(SelectionKey.OP_WRITE);
                }
            } else {
                key.interestOps(SelectionKey.OP_READ);
                selector.wakeup();
            }
        }

        private void write(SelectionKey key) throws IOException {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            Send response = (Send) key.attachment();
            int written = response.writeTo(socketChannel);
            if (response.complete()) {
                key.attach(null);
                key.interestOps(SelectionKey.OP_READ);
            } else {
                key.interestOps(SelectionKey.OP_WRITE);
                selector.wakeup();
            }
        }

        private void close(SelectionKey key) {
            SocketChannel socketChannel = (SocketChannel) key.channel();
            try {
                socketChannel.socket().close();
                socketChannel.close();
                key.attach(null);
                key.cancel();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        void accept(SocketChannel socketChannel) {
            newConnections.add(socketChannel);
            selector.wakeup();
        }

        private void configureNewConnections() throws ClosedChannelException {
            while (newConnections.size() > 0) {
                SocketChannel channel = newConnections.poll();
                channel.register(selector, SelectionKey.OP_READ);
            }
        }

        public void shutdown() {
            running.set(false);
            selector.wakeup();
            try {
                shutdownLatch.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * socket accept server
     */
    private class Acceptor implements Runnable {
        private int port;
        private Processor[] processors;
        private int socketSendBuffer;
        private int socketReceiveBuffer;

        private CountDownLatch startupLatch = new CountDownLatch(1);
        private CountDownLatch shutdownLatch = new CountDownLatch(1);
        private AtomicBoolean running = new AtomicBoolean(true);

        Selector selector;

        public Acceptor(int port, Processor[] processors, int socketSendBuffer, int socketReceiveBuffer) {
            this.port = port;
            this.processors = processors;
            this.socketSendBuffer = socketSendBuffer;
            this.socketReceiveBuffer = socketReceiveBuffer;
            try {
                selector = Selector.open();
            } catch (IOException e) {
                throw new RuntimeException("Acceptor Selector open error", e);
            }
        }

        @Override
        public void run() {
            ServerSocketChannel serverSocketChannel;
            try {
                serverSocketChannel = ServerSocketChannel.open();
                serverSocketChannel.configureBlocking(false);
                serverSocketChannel.socket().bind(new InetSocketAddress(port));
                serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);
                LOGGER.info("Awaiting connections on port " + port);
                running.set(true);
                startupLatch.countDown();

                int currentProcessor = 0;
                while (running.get()) {
                    int ready = selector.select(500);
                    if (ready > 0) {
                        Set<SelectionKey> keys = selector.selectedKeys();
                        Iterator<SelectionKey> it = keys.iterator();
                        try {
                            while (it.hasNext() && running.get()) {
                                SelectionKey key = it.next();
                                it.remove();
                                if (key.isAcceptable()) {
                                    accept(key, processors[currentProcessor]);
                                } else {
                                    throw new IllegalStateException("Unrecognized key state for acceptor thread.");
                                }
                                currentProcessor = (currentProcessor + 1) % processors.length;
                            }
                        } catch (Exception e) {
                            LOGGER.error("Error in acceptor", e);
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Acceptor ServerSocketChannel open error", e);
            }

            LOGGER.debug("Closing server socket and selector.");
            try {
                serverSocketChannel.close();
                selector.close();
            } catch (IOException e) {
                LOGGER.error("Error in close", e);
            }
            shutdownLatch.countDown();
        }

        private void accept(SelectionKey key, Processor processor) throws Exception {
            ServerSocketChannel serverSocketChannel = (ServerSocketChannel) key.channel();
            serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize);
            SocketChannel socketChannel = serverSocketChannel.accept();
            socketChannel.configureBlocking(false);
            socketChannel.socket().setTcpNoDelay(true);
            socketChannel.socket().setSendBufferSize(sendBufferSize);

            processor.accept(socketChannel);
        }

        public void awaitStartup() {
            try {
                startupLatch.await();
            } catch (InterruptedException e) {
                throw new RuntimeException("Acceptor thread interrupted", e);
            }
        }

        public void shutdown() {
            running.set(false);
            selector.wakeup();
            try {
                shutdownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
