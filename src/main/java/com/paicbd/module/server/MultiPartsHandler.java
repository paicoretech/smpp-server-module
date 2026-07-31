package com.paicbd.module.server;

import com.paicbd.module.utils.ConcatenatedMessageDetails;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.SubmitSm;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@RequiredArgsConstructor
public class MultiPartsHandler {
    private final Map<String, ConcatenationAppender> concatenationAppenders = new ConcurrentHashMap<>();

    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final int messagePartsTtl;

    public void messagePartsProcessor(
            MessageEvent incomingEvent,
            ConcatenatedMessageDetails details,
            SubmitSm submitSm,
            String kafkaTopic) {
        String key = generateKey(incomingEvent.getSystemId(), details.msgReferenceNumber());
        log.debug("Processing part: {} - key: {}", Converter.valueAsString(details), key);

        concatenationAppenders.compute(key, (k, existing) -> {
            ConcatenationAppender appender = (existing != null) ? existing : rehydrateFromScylla(k);

            if (appender == null) {
                appender = new ConcatenationAppender(details.totalSegments(), incomingEvent);
                appender.initialize(incomingEvent, details, submitSm);
                log.info("Created new multipart accumulator with key: {}, TTL: {} seconds", k, messagePartsTtl);
            } else {
                appender.addMessagePart(submitSm, details, incomingEvent);
                log.debug("Appended to existing multipart accumulator with key: {}", k);
            }

            if (appender.isComplete()) {
                scyllaManager.deleteSmppMessagePartsByKey(k);
                MessageEvent finalMessage = appender.finalizeEvent();
                kafkaTemplate.send(kafkaTopic, finalMessage.toString());
                log.debug("Multipart message completed and published for key: {}", k);
                return null; // remove from in-memory map atomically
            }

            scyllaManager.insertIntoSmppMessageParts(k, appender.snapshot(), messagePartsTtl);
            return appender;
        });
    }

    private ConcatenationAppender rehydrateFromScylla(String key) {
        String snapshot = scyllaManager.getSmppMessagePartsByKey(key);
        if (Objects.isNull(snapshot)) {
            return null;
        }
        try {
            ConcatenationAppender appender = ConcatenationAppender.restore(snapshot);
            log.info("Rehydrated multipart accumulator from ScyllaDB for key: {} (parts so far: {})",
                    key, appender.getIncomingEvent().getMessageParts().size());
            return appender;
        } catch (Exception e) {
            log.warn("Failed to rehydrate multipart accumulator from ScyllaDB for key: {} (incompatible snapshot, will treat as new). Cause: {}",
                    key, e.getMessage());
            scyllaManager.deleteSmppMessagePartsByKey(key);
            return null;
        }
    }

    private String generateKey(String systemId, int msgReferenceNumber) {
        return systemId + "|" + msgReferenceNumber;
    }
}
