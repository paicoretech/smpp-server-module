package com.paicbd.module.server;

import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.SmppServerConfig;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.ws.SocketSession;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.paicbd.module.utils.Constants.FORCED_STOPPED_STATUS;
import static com.paicbd.module.utils.Constants.STARTED_SERVER;
import static com.paicbd.module.utils.Constants.STARTED_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED_STATUS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@Slf4j
@ExtendWith(MockitoExtension.class)
class SmppServerManagerTest {
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
    ConcurrentMap<Integer, CustomSmppServer> customSmppServerMap;
    @Mock
    ScyllaManager scyllaManager;
    @Mock
    KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    private RateLimiterUtilManager rateLimiterManager;
    @Mock
    private AppProperties appProperties;
    @Mock
    private ProxyResponseHandler proxyResponseHandler;

    @InjectMocks
    private SmppServerManager smppServerManager;

    @Test
    @DisplayName("Init when not exists data in Redis for service providers and custom servers")
    void initWhenNotExistsDataInRedisForServiceProvidersAndCustomServers() {
        when(redisManager.hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME)).thenReturn(Collections.emptyMap());
        when(redisManager.hgetAll(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME)).thenReturn(Collections.emptyMap());

        smppServerManager.startManager();
        verify(redisManager).hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME);
        verify(redisManager).hgetAll(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME);

        verify(providers, never()).addAll(anyCollection());
        verify(customSmppServerMap, never()).put(anyInt(), any(CustomSmppServer.class));
    }

    @Test
    @DisplayName("Init when exists data in Redis for service providers and custom servers")
    void initWhenExistsDataInRedisForServiceProvidersAndCustomServers() {
        providers = new HashSet<>();
        providers = spy(providers);

        customSmppServerMap = new ConcurrentHashMap<>();
        customSmppServerMap = spy(customSmppServerMap);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate, rateLimiterManager,  appProperties, proxyResponseHandler
        );

        when(redisManager.hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME)).thenReturn(fetchServiceProvidersString());
        when(redisManager.hgetAll(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME)).thenReturn(fetchCustomSmppServerString());

        smppServerManager.startManager();
        verify(redisManager).hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME);
        verify(redisManager).hgetAll(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME);

        verify(providers).addAll(anyCollection());
        verify(customSmppServerMap, times(3)).put(anyInt(), any(CustomSmppServer.class));

        int providersSmppMocked = fetchServiceProvidersString().values().stream()
                .map(provider -> Converter.stringToObject(provider, ServiceProvider.class))
                .filter(provider -> "smpp".equalsIgnoreCase(provider.getProtocol()))
                .toList().size();
        assertEquals(providersSmppMocked, providers.size());
        assertEquals(fetchServiceProvidersString().size(), customSmppServerMap.size());

        Map<Integer, ServiceProvider> fetchedProviders = fetchServiceProvidersString().values().stream()
                .map(provider -> Converter.stringToObject(provider, ServiceProvider.class))
                .collect(Collectors.toMap(ServiceProvider::getNetworkId, provider -> provider));

        providers.forEach(provider -> {
            assertTrue(provider.getProtocol().equalsIgnoreCase("smpp"));
            assertEquals(fetchedProviders.get(provider.getNetworkId()).toString(), provider.toString());
        });

        customSmppServerMap.forEach((id, customSmppServer) -> {
            if (Objects.equals(customSmppServer.getSmppServerConfig().getEnabled(), 1)) {
                assertEquals("STARTED", customSmppServer.getSmppServerConfig().getStatus());
            }
        });
    }

    @ParameterizedTest
    @MethodSource("paramsForAddOrUpdateNewConfig")
    @DisplayName("Add or update new config when map contains key for config")
    void addOrUpdateNewConfigWhenMapContainsKeyForConfig(int id, SmppServerConfig smppServerConfig) {
        log.info("id {}, enabled {}, config {}", id, smppServerConfig.getEnabled(), smppServerConfig);
        customSmppServerMap = new ConcurrentHashMap<>();
        customSmppServerMap = spy(customSmppServerMap);
        SmppServerConfig currentConfig = SmppServerConfig.builder()
                .id(id)
                .ip("127.0.0.1")
                .port(34121)
                .status(STOPPED_STATUS)
                .enabled(0)
                .build();
        when(redisManager.hget(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME, String.valueOf(id))).thenReturn(smppServerConfig.toString());
        CustomSmppServer customSmppServer = new CustomSmppServer(
                redisManager, socketSession, providers, currentConfig,
                spSessionMap, generalSettingsCacheConfig, scyllaManager, kafkaTemplate, rateLimiterManager, appProperties, proxyResponseHandler);
        Thread.startVirtualThread(customSmppServer);
        customSmppServerMap.put(id, customSmppServer);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler
        );

        smppServerManager.addOrUpdateSmppServerConfig(id);

        verify(customSmppServerMap).containsKey(id);
        verify(customSmppServerMap, atLeast(1)).get(id);

        switch (smppServerConfig.getEnabled()) {
            case 0 -> {
                assertEquals(STOPPED_STATUS, customSmppServer.getSmppServerConfig().getStatus());
                verify(customSmppServerMap).remove(id);
                assertEquals(0, customSmppServerMap.size());
            }
            case 1 -> {
                assertEquals(STARTED_STATUS, customSmppServer.getSmppServerConfig().getStatus());
                assertEquals(1, customSmppServerMap.size());
            }
            case 2 -> {
                verify(customSmppServerMap).remove(id);
                assertEquals(0, customSmppServerMap.size());
            }
            default -> {
                verify(customSmppServerMap, never()).put(id, null);
                verify(customSmppServerMap, never()).remove(id);
            }
        }

        customSmppServer.stop();
    }

    @ParameterizedTest
    @MethodSource("paramsForAddOrUpdateNewConfigWithoutDataInMap")
    @DisplayName("Add or update new config when map not contains key for config")
    void addOrUpdateNewConfigWhenMapNotContainsKeyForConfig(int id, int enabled) {
        log.info("id {}, enabled {}", id, enabled);
        customSmppServerMap = new ConcurrentHashMap<>();
        customSmppServerMap = spy(customSmppServerMap);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler
        );

        SmppServerConfig smppServerConfig = SmppServerConfig.builder()
                .id(id)
                .ip("127.0.0.1")
                .port(2775)
                .status(enabled == 0 ? STOPPED_STATUS : STARTED_STATUS)
                .enabled(enabled)
                .build();

        when(redisManager.hget(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME, String.valueOf(id))).thenReturn(smppServerConfig.toString());
        smppServerManager.addOrUpdateSmppServerConfig(id);

        verify(customSmppServerMap).containsKey(id);
        verify(customSmppServerMap).put(eq(id), any(CustomSmppServer.class));
        assertEquals(1, customSmppServerMap.size());

        CustomSmppServer customSmppServer = customSmppServerMap.get(id);
        if (Objects.equals(enabled, 1)) {
            assertEquals(STARTED_STATUS, customSmppServer.getSmppServerConfig().getStatus());
        } else {
            assertEquals(STOPPED_STATUS, customSmppServer.getSmppServerConfig().getStatus());
        }
    }

    @Test
    @DisplayName("Stop all SMPP servers")
    void cleanResources() {
        customSmppServerMap = new ConcurrentHashMap<>();
        SmppServerConfig smppServerConfig01 = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(2775)
                .status(STARTED_STATUS)
                .enabled(1)
                .build();

        SmppServerConfig smppServerConfig02 = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(2776)
                .status(STARTED_STATUS)
                .enabled(1)
                .build();

        ConcurrentMap<Integer, CustomSmppServer> customSmppServerCopy = spy(new ConcurrentHashMap<>());
        CustomSmppServer customSmppServer01 =
                new CustomSmppServer(
                        redisManager, socketSession, providers, smppServerConfig01,
                        spSessionMap, generalSettingsCacheConfig, scyllaManager, kafkaTemplate,
                        rateLimiterManager, appProperties, proxyResponseHandler);
        customSmppServer01 = spy(customSmppServer01);
        Thread.startVirtualThread(customSmppServer01);
        customSmppServerCopy.put(1, customSmppServer01);

        CustomSmppServer customSmppServer02 = new CustomSmppServer(
                redisManager, socketSession, providers, smppServerConfig02,
                spSessionMap, generalSettingsCacheConfig, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler);
        customSmppServer02 = spy(customSmppServer02);
        Thread.startVirtualThread(customSmppServer02);
        customSmppServerCopy.put(2, customSmppServer02);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler
        );

        try (ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor()) {
            executor.schedule(() -> log.info("Stopping all SMPP servers"), 3, TimeUnit.SECONDS);
        }

        smppServerManager.cleanResources();
    }

    @Test
    @DisplayName("Force stop all SMPP servers")
    void forceStopAllSmppServers() {
        customSmppServerMap = new ConcurrentHashMap<>();
        SmppServerConfig smppServerConfig01 = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(2778)
                .status(STARTED_STATUS)
                .enabled(1)
                .build();

        SmppServerConfig smppServerConfig02 = SmppServerConfig.builder()
                .id(2)
                .ip("127.0.0.1")
                .port(2779)
                .status(STARTED_STATUS)
                .enabled(1)
                .build();

        ConcurrentMap<Integer, CustomSmppServer> customSmppServerCopy = spy(new ConcurrentHashMap<>());
        CustomSmppServer customSmppServer01 =
                new CustomSmppServer(
                        redisManager, socketSession, providers, smppServerConfig01,
                        spSessionMap, generalSettingsCacheConfig, scyllaManager, kafkaTemplate,
                        rateLimiterManager, appProperties, proxyResponseHandler);
        customSmppServer01 = spy(customSmppServer01);
        Thread.startVirtualThread(customSmppServer01);
        customSmppServerCopy.put(1, customSmppServer01);

        CustomSmppServer customSmppServer02 = new CustomSmppServer(
                redisManager, socketSession, providers, smppServerConfig02,
                spSessionMap, generalSettingsCacheConfig, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler);
        customSmppServer02 = spy(customSmppServer02);
        Thread.startVirtualThread(customSmppServer02);
        customSmppServerCopy.put(2, customSmppServer02);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler
        );

        try (ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor()) {
            executor.schedule(() -> log.info("Force stopping all SMPP servers"), 3, TimeUnit.SECONDS);
        }

        customSmppServer01.forceStopped();
        customSmppServer02.forceStopped();

        assertEquals(FORCED_STOPPED_STATUS, smppServerConfig01.getStatus());
        assertEquals(STARTED_SERVER, smppServerConfig01.getEnabled());
        assertEquals(FORCED_STOPPED_STATUS, smppServerConfig02.getStatus());
        assertEquals(STARTED_SERVER, smppServerConfig02.getEnabled());
    }

    @Test
    @DisplayName("Clean resources correctly stops service providers and custom SMPP servers")
    void cleanResourcesShouldStopAllProvidersAndCustomSmppServers() {
        ServiceProvider provider1 = ServiceProvider.builder()
                .networkId(1)
                .status(STARTED_STATUS)
                .enabled(1)
                .build();
        ServiceProvider provider2 = ServiceProvider.builder()
                .networkId(2)
                .status(STARTED_STATUS)
                .enabled(1)
                .build();

        Set<ServiceProvider> mockedProviders = new HashSet<>();
        mockedProviders.add(provider1);
        mockedProviders.add(provider2);

        when(socketSession.getStompSession()).thenReturn(mock(StompSession.class));
        this.providers = spy(mockedProviders);

        SmppServerConfig config1 = SmppServerConfig.builder()
                .id(1)
                .status(STARTED_STATUS)
                .enabled(1)
                .name("server1")
                .build();

        SmppServerConfig config2 = SmppServerConfig.builder()
                .id(2)
                .status(STARTED_STATUS)
                .enabled(1)
                .port(2776)
                .build();

        CustomSmppServer mockServer1 = mock(CustomSmppServer.class);
        CustomSmppServer mockServer2 = mock(CustomSmppServer.class);

        when(mockServer1.getSmppServerConfig()).thenReturn(config1);
        when(mockServer2.getSmppServerConfig()).thenReturn(config2);

        ConcurrentMap<Integer, CustomSmppServer> mockedServerMap = new ConcurrentHashMap<>();
        mockedServerMap.put(1, mockServer1);
        mockedServerMap.put(2, mockServer2);

        this.customSmppServerMap = spy(mockedServerMap);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler
        );

        smppServerManager.cleanResources();

        verify(providers, times(1)).forEach(any());
        assertTrue(providers.stream().allMatch(p -> FORCED_STOPPED_STATUS.equals(p.getStatus())));

        verify(redisManager, times(2)).hset(
                eq(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME),
                anyString(),
                anyString()
        );

        verify(customSmppServerMap, times(1)).entrySet();
        verify(mockServer1, times(1)).forceStopped();
        verify(mockServer2, times(1)).forceStopped();
        assertTrue(customSmppServerMap.isEmpty());
    }

    @Test
    @DisplayName("Update existing smpp server config with tlsEnabled true propagates to server config")
    void addOrUpdateSmppServerConfigWithTlsEnabledTruePropagatesCorrectly() {
        int id = 10;
        customSmppServerMap = new ConcurrentHashMap<>();
        customSmppServerMap = spy(customSmppServerMap);

        SmppServerConfig currentConfig = SmppServerConfig.builder()
                .id(id)
                .ip("127.0.0.1")
                .port(34250)
                .status(STOPPED_STATUS)
                .enabled(0)
                .build();

        SmppServerConfig updatedConfig = SmppServerConfig.builder()
                .id(id)
                .ip("127.0.0.1")
                .port(34250)
                .status(STARTED_STATUS)
                .enabled(1)
                .tlsEnabled(true)
                .build();

        when(redisManager.hget(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME, String.valueOf(id)))
                .thenReturn(updatedConfig.toString());

        CustomSmppServer customSmppServer = new CustomSmppServer(
                redisManager, socketSession, providers, currentConfig,
                spSessionMap, generalSettingsCacheConfig, scyllaManager, kafkaTemplate, rateLimiterManager, appProperties, proxyResponseHandler);
        Thread.startVirtualThread(customSmppServer);
        customSmppServerMap.put(id, customSmppServer);

        smppServerManager = new SmppServerManager(
                redisManager, socketSession, providers, spSessionMap,
                generalSettingsCacheConfig, customSmppServerMap, scyllaManager, kafkaTemplate,
                rateLimiterManager, appProperties, proxyResponseHandler
        );

        smppServerManager.addOrUpdateSmppServerConfig(id);

        assertTrue(customSmppServer.getSmppServerConfig().isTlsEnabled());
        assertEquals(STARTED_STATUS, customSmppServer.getSmppServerConfig().getStatus());

        customSmppServer.stop();
    }

    static Map<String, String> fetchServiceProvidersString() {
        ServiceProvider sp01 = ServiceProvider.builder()
                .networkId(1)
                .protocol("smpp")
                .systemId("systemId01")
                .password("password01")
                .systemType("systemType01")
                .addressNpi(1)
                .addressRange("addressRange01")
                .validity(120)
                .status(STARTED_STATUS)
                .build();
        ServiceProvider sp02 = ServiceProvider.builder()
                .networkId(2)
                .protocol("smpp")
                .systemId("systemId02")
                .password("password02")
                .systemType("systemType02")
                .addressNpi(2)
                .addressRange("addressRange02")
                .validity(120)
                .status(STOPPED_STATUS)
                .build();
        ServiceProvider sp03 = ServiceProvider.builder()
                .networkId(3)
                .protocol("http")
                .systemId("systemId03")
                .password("password03")
                .systemType("systemType03")
                .addressNpi(3)
                .addressRange("addressRange03")
                .validity(120)
                .status(STARTED_STATUS)
                .build();
        return Map.of("1", sp01.toString(), "2", sp02.toString(), "3", sp03.toString());
    }

    static Map<String, String> fetchCustomSmppServerString() {
        SmppServerConfig config01 = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(2775)
                .status(STOPPED_STATUS)
                .enabled(0)
                .name("name01")
                .processorDegree(1)
                .queueCapacity(1)
                .build();
        SmppServerConfig config02 = SmppServerConfig.builder()
                .id(2)
                .ip("127.0.0.1")
                .port(2776)
                .status(STARTED_STATUS)
                .enabled(1)
                .name("name02")
                .processorDegree(2)
                .queueCapacity(2)
                .build();
        SmppServerConfig config03 = SmppServerConfig.builder()
                .id(3)
                .ip("127.0.0.1")
                .port(2777)
                .status(STARTED_STATUS)
                .enabled(2)
                .name("name03")
                .processorDegree(3)
                .queueCapacity(3)
                .build();
        return Map.of("1", config01.toString(), "2", config02.toString(), "3", config03.toString());
    }

    static Stream<Arguments> paramsForAddOrUpdateNewConfig() {
        SmppServerConfig config01 = SmppServerConfig.builder()
                .id(1)
                .ip("127.0.0.1")
                .port(34121)
                .status(STOPPED_STATUS)
                .enabled(0)
                .build();
        SmppServerConfig config02 = SmppServerConfig.builder()
                .id(2)
                .ip("127.0.0.1")
                .port(34121)
                .status(STOPPED_STATUS)
                .enabled(1)
                .build();
        SmppServerConfig config03 = SmppServerConfig.builder()
                .id(3)
                .ip("127.0.0.1")
                .port(34121)
                .status(STARTED_STATUS)
                .enabled(2)
                .build();
        SmppServerConfig config04 = SmppServerConfig.builder()
                .id(4)
                .ip("127.0.0.1")
                .port(34121)
                .status(STOPPED_STATUS)
                .enabled(4)
                .build();
        return Stream.of(
                Arguments.of(1, config01),
                Arguments.of(2, config02),
                Arguments.of(3, config03),
                Arguments.of(4, config04)
        );
    }

    static Stream<Arguments> paramsForAddOrUpdateNewConfigWithoutDataInMap() {
        return Stream.of(
                Arguments.of(1, 0),
                Arguments.of(2, 1)
        );
    }
}
