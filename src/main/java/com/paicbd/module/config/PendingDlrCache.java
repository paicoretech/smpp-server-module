package com.paicbd.module.config;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.StaticMethods;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.utils.Converter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PendingDlrCache {
    private final Cache<String, String> cache;
    private final boolean enabled;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public PendingDlrCache(AppProperties appProperties, KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
        this.enabled = appProperties.isPendingDlrCacheEnabled();
        this.cache = Caffeine.newBuilder()
                .maximumSize(appProperties.getPendingDlrCacheMaximumSize())
                .removalListener((String key, String value, RemovalCause cause) -> {
                    if (cause == RemovalCause.SIZE && value != null) {
                        StaticMethods.publishPendingDlrFailureCdr(
                                Converter.stringToObject(value, MessageEvent.class), kafkaTemplate);
                    }
                    log.debug("Pending DLR entry {} removed from cache due to {}", key, cause);
                })
                .build();
    }

    public void put(MessageEvent event) {
        if (!enabled) {
            log.warn("Pending DLR cache is disabled; dropping deliver_sm with messageId {}", event.getMessageId());
            StaticMethods.publishPendingDlrFailureCdr(event, kafkaTemplate);
            return;
        }
        cache.put(event.getMessageId(), event.toString());
    }

    public Collection<String> drainAll() {
        Map<String, String> snapshot = new HashMap<>(cache.asMap());
        cache.invalidateAll(snapshot.keySet());
        return snapshot.values();
    }

    public void publishPendingFailureCdrsAndClear() {
        cache.asMap().values().forEach(raw ->
                StaticMethods.publishPendingDlrFailureCdr(
                        Converter.stringToObject(raw, MessageEvent.class), kafkaTemplate));
        cache.invalidateAll();
    }

    void cleanUp() {
        cache.cleanUp();
    }
}
