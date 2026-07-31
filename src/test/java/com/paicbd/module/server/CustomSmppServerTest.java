package com.paicbd.module.server;

import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.SmppServerConfig;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.ws.SocketSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.module.utils.Constants.STARTED_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED_STATUS;
import static com.paicbd.module.utils.Constants.UPDATE_SMPP_SERVER_STATUS_NOTIFICATION;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CustomSmppServerTest {

    @Mock
    RedisManager redisManager;
    @Mock
    SocketSession socketSession;
    @Mock
    Set<ServiceProvider> providers;
    @Mock
    ConcurrentMap<Integer, SpSession> spSessionMap;
    @Mock
    GeneralSettingsCacheConfig generalSettingsCacheConfig;
    @Mock
    ScyllaManager scyllaManager;
    @Mock
    KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    RateLimiterUtilManager rateLimiterManager;
    @Mock
    AppProperties appProperties;
    @Mock
    ProxyResponseHandler proxyResponseHandler;

    @Test
    @DisplayName("updateSmppServerConfig propagates tlsEnabled and all other fields to the running configuration")
    void updateSmppServerConfigPropagatesTlsEnabledAndAllOtherFieldsToRunningConfig() {
        SmppServerConfig initialConfig = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(2775)
                .name("server-original")
                .processorDegree(1)
                .queueCapacity(100)
                .transactionTimer(2000)
                .waitForBind(2000)
                .status(STOPPED_STATUS)
                .enabled(0)
                .tlsEnabled(false)
                .build();

        CustomSmppServer server = new CustomSmppServer(
                redisManager, socketSession, providers, initialConfig,
                spSessionMap, generalSettingsCacheConfig, scyllaManager,
                kafkaTemplate, rateLimiterManager, appProperties, proxyResponseHandler);

        SmppServerConfig updatedConfig = SmppServerConfig.builder()
                .id(1)
                .ip("10.0.0.1")
                .port(2776)
                .name("server-updated")
                .processorDegree(4)
                .queueCapacity(1000)
                .transactionTimer(5000)
                .waitForBind(5000)
                .status(STARTED_STATUS)
                .enabled(1)
                .tlsEnabled(true)
                .build();

        server.updateSmppServerConfig(updatedConfig);

        SmppServerConfig result = server.getSmppServerConfig();
        assertTrue(result.isTlsEnabled());
        assertEquals("server-updated", result.getName());
        assertEquals("10.0.0.1", result.getIp());
        assertEquals(2776, result.getPort());
        assertEquals(4, result.getProcessorDegree());
        assertEquals(1000, result.getQueueCapacity());
        assertEquals(5000, result.getTransactionTimer());
        assertEquals(5000, result.getWaitForBind());
        assertEquals(1, result.getEnabled());
    }

    @Test
    @DisplayName("run with TLS enabled and blank keystore path sends FAILED notification with TLS error message")
    void runWithTlsEnabledAndBlankKeystorePathSendsFailedNotification() {
        StompSession stompSession = mock(StompSession.class);
        when(socketSession.getStompSession()).thenReturn(stompSession);
        when(appProperties.getTlsKeystorePath()).thenReturn("");

        SmppServerConfig config = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(35100)
                .status(STOPPED_STATUS)
                .enabled(1)
                .tlsEnabled(true)
                .build();

        CustomSmppServer server = new CustomSmppServer(
                redisManager, socketSession, providers, config,
                spSessionMap, generalSettingsCacheConfig, scyllaManager,
                kafkaTemplate, rateLimiterManager, appProperties, proxyResponseHandler);

        Thread.startVirtualThread(server);

        await().timeout(Duration.ofSeconds(5))
                .untilAsserted(() -> {
                    verify(stompSession, atLeast(1)).send(
                            eq(UPDATE_SMPP_SERVER_STATUS_NOTIFICATION), contains("FAILED"));
                    verify(stompSession, atLeast(1)).send(
                            eq(UPDATE_SMPP_SERVER_STATUS_NOTIFICATION), contains("TLS configuration error"));
                });
    }
}
