package com.paicbd.module.components;

import com.paicbd.module.utils.SpSession;
import com.paicbd.module.utils.StaticMethods;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.exception.RTException;
import com.paicbd.smsc.kafka.KafkaConsumerConstants;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Watcher;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.session.SMPPServerSession;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliverSmQueueConsumer {
    private final AtomicInteger deliverSmCounterPerSecond = new AtomicInteger(0);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ConcurrentMap<Integer, SpSession> spSessionMap;
    private final GeneralSettingsCacheConfig generalSettingsCacheConfig;

    @PostConstruct
    private void startQueueProcessing() {
        log.warn("Starting DeliverSmQueueConsumer");
        Thread.startVirtualThread(() -> new Watcher("SmppServer:OutboundDeliverSm", deliverSmCounterPerSecond, 1));
    }

    @KafkaListener(
            topics = KafkaTopicsConstants.SMPP_DLR_TOPIC,
            groupId = KafkaConsumerConstants.SMPP_DLR_GROUP_ID)
    public void handleDeliverySms(List<String> messages) {
        if (Objects.isNull(messages) || messages.isEmpty()) {
            log.debug("Received empty or null messages in handleDeliverySms");
            return;
        }

        Flux.fromIterable(messages)
                .map(x -> Converter.stringToObject(x, MessageEvent.class))
                .doOnNext(this::processDeliverSm)
                .subscribe();
    }

    private void processDeliverSm(MessageEvent deliverSmEvent) {
        try {
            if (Objects.isNull(deliverSmEvent)) {
                log.error("Deliver_sm event is null");
                return ;
            }

            int networkId = deliverSmEvent.getDestNetworkId();
            SpSession spSession = spSessionMap.get(networkId);
            if (Objects.isNull(spSession)) {
                log.warn("No SpSession for networkId {}; publishing FAILED CDR for deliver_sm {}", networkId, deliverSmEvent.getMessageId());
                StaticMethods.publishPendingDlrFailureCdr(deliverSmEvent, kafkaTemplate);
                return;
            }

            SMPPServerSession serverSession = (SMPPServerSession) spSession.getNextRoundRobinSession();
            if (serverSession != null) {
                GeneralSettings smppGeneralSettings = generalSettingsCacheConfig.getCurrentGeneralSettings();
                boolean dlrTlvEnabled = spSession.getCurrentServiceProvider().isDlrTlvEnabled();
                StaticMethods.sendDeliverSm(serverSession, deliverSmEvent, dlrTlvEnabled, smppGeneralSettings, kafkaTemplate);
                deliverSmCounterPerSecond.getAndIncrement();
            } else {
                log.debug("No active session RECEIVER/TRANSCEIVER to send deliver_sm with id {}", deliverSmEvent.getId());
                spSession.getPendingDlrCache().put(deliverSmEvent);
            }
        } catch (Exception e) {
            log.error("Error on process deliverSm {} on method processDeliverSm", e.getMessage());
            throw new RTException("Error on process deliverSm");
        }
    }
}
