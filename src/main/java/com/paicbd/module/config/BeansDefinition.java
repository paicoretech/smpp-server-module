package com.paicbd.module.config;

import com.paicbd.module.server.CustomSmppServer;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import com.paicbd.smsc.utils.RedisManager;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

@Generated
@Configuration
@RequiredArgsConstructor
public class BeansDefinition {
    private final AppProperties appProperties;

    private final KafkaTemplate<String, String> kafkaTemplate;

    @Bean
    public Set<ServiceProvider> providers() {
        return ConcurrentHashMap.newKeySet();
    }

    @Bean
    public ConcurrentMap<Integer, SpSession> spSessionMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, String> networkIdSystemIdMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public ConcurrentMap<Integer, CustomSmppServer> customSmppServerMap() {
        return new ConcurrentHashMap<>();
    }

    @Bean
    public RedisManager redisManager() {
        return Converter.paramsToRedisManager(
                new UtilsRecords.JedisConfigParams(appProperties.getRedisNodes(), appProperties.getRedisMaxTotal(),
                        appProperties.getRedisMaxIdle(), appProperties.getRedisMinIdle(),
                        appProperties.isRedisBlockWhenExhausted(), appProperties.getRedisConnectionTimeout(),
                        appProperties.getRedisSoTimeout(), appProperties.getRedisMaxAttempts(),
                        appProperties.getRedisUser(), appProperties.getRedisPassword(),
                        appProperties.isRedisStandaloneEnabled())
        );
    }

    @Bean
    public SocketSession socketSession() {
        return new SocketSession("sp");
    }

    @Bean
    public ScyllaManager scyllaDbManager() {
        return new ScyllaManager(
                appProperties.getContactPoints(),
                appProperties.getLocalDatacenter(),
                appProperties.getUsername(),
                appProperties.getPassword(),
                false);
    }

    @Bean
    public RateLimiterUtilManager rateLimiterManager() {
        return new RateLimiterUtilManager(appProperties.isKafkaModeEnabled() ? kafkaTemplate : null);
    }
}
