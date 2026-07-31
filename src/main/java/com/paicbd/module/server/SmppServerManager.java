package com.paicbd.module.server;

import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.BindEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.SmppServerConfig;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.ws.SocketSession;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import com.paicbd.smsc.utils.RedisManager;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.module.utils.Constants.DELETED_SERVER;
import static com.paicbd.module.utils.Constants.FORCED_STOPPED_STATUS;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_STATUS;
import static com.paicbd.module.utils.Constants.STARTED_SERVER;
import static com.paicbd.module.utils.Constants.STARTED_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED;
import static com.paicbd.module.utils.Constants.STOPPED_SERVER;
import static com.paicbd.module.utils.Constants.TYPE;
import static com.paicbd.module.utils.Constants.WEBSOCKET_STATUS_ENDPOINT;

@Slf4j
@Component
@RequiredArgsConstructor
public class SmppServerManager {
    private final RedisManager redisManager;
    private final SocketSession socketSession;
    private final Set<ServiceProvider> providers;
    private final ConcurrentMap<Integer, SpSession> spSessionMap;
    private final GeneralSettingsCacheConfig generalSettingsCacheConfig;
    private final ConcurrentMap<Integer, CustomSmppServer> customSmppServerMap;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RateLimiterUtilManager rateLimiterUtilManager;
    private final AppProperties appProperties;
    private final ProxyResponseHandler proxyResponseHandler;

    @EventListener(ApplicationReadyEvent.class)
    public void startManager() {
        loadServiceProviders();
        publishStartupUnbindEvents();
        loadCustomSmppServer();
    }

    private void loadServiceProviders() {
        Map<String, String> redisProviders = redisManager.hgetAll(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME);
        List<ServiceProvider> providersToAdd = new ArrayList<>();

        redisProviders.forEach((networkId, spInRaw) -> {
            spInRaw = spInRaw.replace("\\", "\\\\");
            ServiceProvider serviceProvider = Converter.stringToObject(spInRaw, ServiceProvider.class);
            Assert.notNull(serviceProvider, "An error occurred while casting ServiceProvider in loadServiceProviders. The value is incompatible with the expected class");

            if ("smpp".equalsIgnoreCase(serviceProvider.getProtocol())) {
                if (FORCED_STOPPED_STATUS.equalsIgnoreCase(serviceProvider.getStatus())) {
                    log.info("Resuming SMPP Service Provider (FORCED_STOP) networkId={} systemId={}",
                            serviceProvider.getNetworkId(), serviceProvider.getSystemId());
                    serviceProvider.setStatus(STARTED_STATUS);
                    serviceProvider.setEnabled(1);
                    redisManager.hset(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME,
                            String.valueOf(serviceProvider.getNetworkId()), Converter.valueAsString(serviceProvider));
                    if (Objects.nonNull(socketSession.getStompSession())) {
                        socketSession.getStompSession().send(WEBSOCKET_STATUS_ENDPOINT,
                                String.format("%s,%s,%s,%s", "sp", serviceProvider.getNetworkId(), "status", STARTED_STATUS));
                    }
                }
                providersToAdd.add(serviceProvider);
            }
        });

        if (!providersToAdd.isEmpty()) {
            this.providers.addAll(providersToAdd);
        }
        log.info("Loaded {} SMPP Service Providers", providersToAdd.size());
    }

    private void publishStartupUnbindEvents() {
        if (!appProperties.isBindNotificationEnabled()) {
            return;
        }
        long enabledCount = providers.stream().filter(sp -> sp.getEnabled() == 1).count();
        providers.stream()
                .filter(sp -> sp.getEnabled() == 1)
                .forEach(sp -> {
                    BindEvent event = BindEvent.builder()
                            .eventType("UNBIND")
                            .networkId(sp.getNetworkId())
                            .systemId(sp.getSystemId())
                            .host(appProperties.getInstanceIp())
                            .sessionId("")
                            .timestamp(System.currentTimeMillis())
                            .build();
                    kafkaTemplate.send(KafkaTopicsConstants.SMPP_BIND_EVENTS_TOPIC, event.toString());
                });
        log.info("Published startup UNBIND events for {} enabled SMPP providers", enabledCount);
    }

    private void loadCustomSmppServer() {
        Map<String, String> redisSmppServerConfigs = redisManager.hgetAll(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME);
        for (Map.Entry<String, String> entry : redisSmppServerConfigs.entrySet()) {
            String smppServerConfigInRaw = entry.getValue().replace("\\", "\\\\");
            SmppServerConfig smppServerConfig = Converter.stringToObject(smppServerConfigInRaw, SmppServerConfig.class);
            Assert.notNull(smppServerConfig, "An error occurred while casting SmppServerConfig, the value is incompatible with the expected class");

            CustomSmppServer customSmppServer =
                    new CustomSmppServer(redisManager, socketSession,
                            providers, smppServerConfig, spSessionMap,
                            generalSettingsCacheConfig, scyllaManager, kafkaTemplate, rateLimiterUtilManager, appProperties,
                            proxyResponseHandler);
            customSmppServerMap.put(smppServerConfig.getId(), customSmppServer);
            if (Objects.equals(smppServerConfig.getEnabled(), STARTED_SERVER) &&
                    (FORCED_STOPPED_STATUS.equals(smppServerConfig.getStatus()) ||
                            STARTED_STATUS.equals(smppServerConfig.getStatus()))) {
                log.info("Starting CustomSmppServer with id {} while loading CustomSmppServers", smppServerConfig.getId());
                Thread.startVirtualThread(customSmppServer);
            } else {
                log.debug("SMPP Server: '{}' (ID: {}) is stopped, not auto-restarting", smppServerConfig.getName(), smppServerConfig.getId());
            }
        }
    }

    public void addOrUpdateSmppServerConfig(int id) {
        String smppServerConfigInRaw = redisManager.hget(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME, String.valueOf(id));
        Objects.requireNonNull(smppServerConfigInRaw, "Redis returned null while getting SmppServerConfig in addOrUpdateSmppServerConfig");
        log.info("Received SmppServerConfig from Redis: {}", smppServerConfigInRaw);

        SmppServerConfig newConfig = Converter.stringToObject(smppServerConfigInRaw, SmppServerConfig.class);
        Assert.notNull(newConfig, "An error occurred while casting SmppServerConfig in addOrUpdateSmppServerConfig");

        if (customSmppServerMap.containsKey(id)) {
            log.debug("The smpp server with id {} exists in the application map", id);
            CustomSmppServer currentCustomSmppServer = customSmppServerMap.get(id);
            switch (newConfig.getEnabled()) {
                case STOPPED_SERVER -> stopSmppServer(id, currentCustomSmppServer);
                case STARTED_SERVER -> startSmppServer(id, currentCustomSmppServer, newConfig);
                case DELETED_SERVER -> deleteSmppServer(id, currentCustomSmppServer);
                default -> log.warn("The updated status of the server is not valid");
            }
            return;
        }

        log.debug("The smpp server with id {} does not exist in the application map, creating a new one", id);
        CustomSmppServer customSmppServer =
                new CustomSmppServer(redisManager, socketSession,
                        providers, newConfig, spSessionMap,
                        generalSettingsCacheConfig, scyllaManager, kafkaTemplate, rateLimiterUtilManager, appProperties,
                        proxyResponseHandler);
        if (Objects.equals(newConfig.getEnabled(), STARTED_SERVER)) {
            log.info("Starting CustomSmppServer with id {} when adding or updating a new SmppServerConfig", id);
            Thread.startVirtualThread(customSmppServer);
        }
        customSmppServerMap.put(id, customSmppServer);
    }

    private void stopSmppServer(int id, CustomSmppServer currentSmppServer) {
        currentSmppServer.stop();
        customSmppServerMap.remove(id); // Remove because customSmppServer extends Runnable, then must be recreated
    }

    private void startSmppServer(int id, CustomSmppServer currentSmppServer, SmppServerConfig newConfig) {
        currentSmppServer.updateSmppServerConfig(newConfig);
        currentSmppServer.getSmppServerConfig().setStatus(STARTED_STATUS);
        currentSmppServer.getSmppServerConfig().setEnabled(STOPPED_SERVER);
        Thread.startVirtualThread(customSmppServerMap.get(id));
    }

    private void deleteSmppServer(int id, CustomSmppServer currentSmppServer) {
        currentSmppServer.stop();
        customSmppServerMap.remove(id);
        redisManager.hdel(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME, String.valueOf(id));
    }

    @PreDestroy
    public void cleanResources() {
        providers.forEach(provider -> {
            provider.setBinds(List.of());
            provider.setCurrentBindsCount(0);
            if (STARTED_STATUS.equalsIgnoreCase(provider.getStatus())) {
                provider.setStatus(FORCED_STOPPED_STATUS);
            }
            redisManager.hset(
                    GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME,
                    String.valueOf(provider.getNetworkId()),
                    Converter.valueAsString(provider)
            );
            if (Objects.nonNull(socketSession.getStompSession())) {
                String statusToSend = FORCED_STOPPED_STATUS.equals(provider.getStatus()) ? FORCED_STOPPED_STATUS : STOPPED;
                socketSession.getStompSession().send(
                        WEBSOCKET_STATUS_ENDPOINT,
                        String.format("%s,%s,%s,%s", TYPE, provider.getNetworkId(), PARAM_UPDATE_STATUS, statusToSend)
                );
            }
            log.info("ServiceProvider {} status persisted on shutdown", provider.getNetworkId());
        });
        customSmppServerMap.entrySet().removeIf(entry -> {
            int id = entry.getKey();
            CustomSmppServer customSmppServer = entry.getValue();
            SmppServerConfig smppServerConfig = customSmppServer.getSmppServerConfig();
            if (Objects.nonNull(smppServerConfig) && STARTED_STATUS.equals(smppServerConfig.getStatus())) {
                customSmppServer.forceStopped();
                log.info("Force stopping CustomSmppServer with id {}", id);
            }
            return true;
        });
        log.info("All enabled CustomSmppServers have been stopped");
    }
}
