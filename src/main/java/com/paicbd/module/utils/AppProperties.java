package com.paicbd.module.utils;

import com.paicbd.smsc.utils.Generated;
import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;

@Getter
@Generated
@Component
public class AppProperties {
    // Redis
    @Value("#{'${redis.cluster.nodes}'.split(',')}")
    private List<String> redisNodes;

    @Value("${redis.threadPool.maxTotal}")
    private int redisMaxTotal;

    @Value("${redis.threadPool.maxIdle}")
    private int redisMaxIdle;

    @Value("${redis.threadPool.minIdle}")
    private int redisMinIdle;

    @Value("${redis.threadPool.blockWhenExhausted}")
    private boolean redisBlockWhenExhausted;

    @Value("${redis.connection.timeout:0}")
    private int redisConnectionTimeout;

    @Value("${redis.so.timeout:0}")
    private int redisSoTimeout;

    @Value("${redis.maxAttempts:0}")
    private int redisMaxAttempts;

    @Value("${redis.connection.password:}")
    private String redisPassword;

    @Value("${redis.connection.user:}")
    private String redisUser;

    @Value("${redis.standalone.enabled:false}")
    private boolean redisStandaloneEnabled;

    // Websocket
    @Value("${websocket.server.host}")
    private String wsHost;

    @Value("${websocket.server.port}")
    private int wsPort;

    @Value("${websocket.server.path}")
    private String wsPath;

    @Value("${websocket.server.enabled}")
    private boolean wsEnabled;

    @Value("${websocket.header.name}")
    private String websocketHeaderName;

    @Value("${websocket.header.value}")
    private String websocketHeaderValue;

    @Value("${websocket.retry.intervalSeconds}")
    private int websocketRetryInterval; // seconds

    @Value("${spring.application.name}")
    private String instanceName;

    @Value("${server.ip}")
    private String instanceIp;

    @Value("${server.port}")
    private String instancePort;

    @Value("${instance.initial.status}")
    private String instanceInitialStatus;

    @Value("${instance.protocol}")
    private String instanceProtocol;

    @Value("${scylla.contact.points}")
    private String contactPoints;

    @Value("${scylla.datacenter}")
    private String localDatacenter;

    @Value("${scylla.user}")
    private String username;

    @Value("${scylla.password}")
    private String password;

    @Value("${spring.kafka.bootstrap-servers}")
    private String kafkaBootstrapServers;

    @Value("${spring.kafka.listener.concurrency}")
    private int kafkaListenerConcurrency;

    @Value("${server.rate.limiter.kafka.emit}")
    private boolean kafkaModeEnabled;

    @Value("${message.parts.ttl:30}")
    private int messagePartsTtl;


    @Value("${bind.notification.enabled:false}")
    private boolean bindNotificationEnabled;

    @Value("${pending.dlr.cache.maximumSize:100000}")
    private int pendingDlrCacheMaximumSize;

    @Value("${pending.dlr.cache.enabled:true}")
    private boolean pendingDlrCacheEnabled;

    @Value("${smpp.server.tls.keystore.path:}")
    private String tlsKeystorePath;

    @Value("${smpp.server.tls.keystore.password:}")
    private String tlsKeystorePassword;

    @Value("${spring.kafka.consumer.reconnect.backoff.ms:1000}")
    private long kafkaConsumerReconnectBackoffMs;

    @Value("${spring.kafka.consumer.reconnect.backoff.max.ms:10000}")
    private long kafkaConsumerReconnectBackoffMaxMs;

    @Value("${spring.kafka.consumer.session.timeout.ms:30000}")
    private int kafkaConsumerSessionTimeoutMs;

    @Value("${spring.kafka.consumer.heartbeat.interval.ms:10000}")
    private int kafkaConsumerHeartbeatIntervalMs;

    @Value("${spring.kafka.producer.reconnect.backoff.ms:1000}")
    private long kafkaProducerReconnectBackoffMs;

    @Value("${spring.kafka.producer.reconnect.backoff.max.ms:10000}")
    private long kafkaProducerReconnectBackoffMaxMs;

    @Value("${proxy.mode.responseTimeout:5000}")
    private long proxyModeResponseTimeout;
}
