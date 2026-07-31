package com.paicbd.module.components;

import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

import static com.paicbd.smsc.kafka.KafkaConsumerConstants.HTTP_PROXY_GROUP_ID_PREFIX;

@Slf4j
@Component
@RequiredArgsConstructor

public class ProxyResponseListener {
    private final ProxyResponseHandler proxyResponseHandler;

    // Module-specific consumer group: both proxy listeners (this module and http-client) share the topic,
    // so each module must consume ALL messages and ignore the ids it did not register.
    @KafkaListener(
            topics = KafkaTopicsConstants.HTTP_PROXY_TOPIC,
            groupId = HTTP_PROXY_GROUP_ID_PREFIX + ".smpp-server",
            concurrency = "1")
    public void listenProxyResponses(List<String> messages) {
        if (Objects.isNull(messages) || messages.isEmpty()) {
            return;
        }

        for (String message : messages) {
            UtilsRecords.HttpProxyResponse proxyResponse = Converter.stringToObject(message, UtilsRecords.HttpProxyResponse.class);
            if (Objects.isNull(proxyResponse) || Objects.isNull(proxyResponse.messageId())) {
                log.warn("Discarding invalid proxy response: {}", message);
                continue;
            }
            log.debug("Received proxy response for message id {}", proxyResponse.messageId());
            //When the HTTP Response comes, we complete the future
            proxyResponseHandler.completeFuture(proxyResponse.messageId(), proxyResponse);
        }
    }
}
