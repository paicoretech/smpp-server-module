package com.paicbd.module.server;

import com.paicbd.module.utils.SpSession;
import com.paicbd.module.utils.StaticMethods;
import com.paicbd.smsc.dto.BindEvent;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.Generated;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.SMPPServerSession;
import org.jsmpp.session.Session;
import org.jsmpp.session.SessionStateListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.util.EnumSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.paicbd.module.utils.Constants.BINDING;
import static com.paicbd.module.utils.Constants.BOUND;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_SESSIONS;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_STATUS;
import static com.paicbd.module.utils.Constants.STARTED;
import static com.paicbd.module.utils.Constants.STOPPED;
import static com.paicbd.module.utils.Constants.TYPE;
import static com.paicbd.module.utils.Constants.UNBINDING;
import static com.paicbd.module.utils.Constants.WEBSOCKET_STATUS_ENDPOINT;

@Slf4j
public class SessionStateListenerImpl implements SessionStateListener {
    private static final EnumSet<SessionState> BOUND_STATES = EnumSet.of(
            SessionState.BOUND_RX,
            SessionState.BOUND_TX,
            SessionState.BOUND_TRX
    );

    private final SpSession spSession;
    private final StompSession stompSession;
    private final RedisManager redisManager;
    private final GeneralSettings smppGeneralSettings;
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final boolean notifyWs;
    private final boolean bindNotificationEnabled;
    private final ServiceProvider currentProvider;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public SessionStateListenerImpl(Integer networkId, ConcurrentMap<Integer, SpSession> spSessionMap,
                                    StompSession stompSession, RedisManager redisManager,
                                    GeneralSettings smppGeneralSettings,
                                    KafkaTemplate<String, String> kafkaTemplate,
                                    boolean bindNotificationEnabled) {
        this.stompSession = stompSession;
        this.redisManager = redisManager;
        this.smppGeneralSettings = smppGeneralSettings;
        this.kafkaTemplate = kafkaTemplate;
        this.spSession = spSessionMap.get(networkId);
        this.currentProvider = spSession.getCurrentServiceProvider();
        this.notifyWs = stompSession != null;
        this.bindNotificationEnabled = bindNotificationEnabled;
        log.info("SessionStateListenerImpl created for networkId {}", networkId);
    }

    @Override
    public void onStateChange(SessionState newState, SessionState oldState, Session source) {
        synchronized (spSession) {
            log.debug("SMPP session state changed from {} to {} for session {}", oldState, newState, source.getSessionId());

            if (STOPPED.equalsIgnoreCase(spSession.getCurrentServiceProvider().getStatus())) {
                log.warn("Service provider {} is stopped, ignoring state change", currentProvider.getSystemId());
                return;
            }

            if (isBoundState(newState)) {
                boundStateProcessor(source);
                this.updateOnRedis();
            } else if (newState == SessionState.CLOSED) {
                closeStateProcessor(source);
                this.updateOnRedis();
            }
        }
    }

    private boolean isBoundState(SessionState state) {
        return BOUND_STATES.contains(state);
    }

    private void sendStompMessage(String param, String value) {
        synchronized (stompSession) {
            String message = this.message(String.valueOf(currentProvider.getNetworkId()), param, value);
            log.info(WEBSOCKET_STATUS_ENDPOINT + " -> {}", message);
            stompSession.send(WEBSOCKET_STATUS_ENDPOINT, message);
        }
    }

    private String message(String networkId, String param, String value) {
        return String.format("%s,%s,%s,%s", TYPE, networkId, param, value);
    }

    public void updateOnRedis() {
        String data = currentProvider.toString();
        //Using this to skip backslash coming from regex in redis
        data = data.replace("\\\\", "\\");
        redisManager.hset("service_providers", String.valueOf(currentProvider.getNetworkId()), data);
    }

    private void boundStateProcessor(Session source) {
        spSession.getCurrentSmppSessions().add(source);
        if (spSession.getCurrentServiceProvider().getCurrentBindsCount() == 0) { // First bind request
            this.currentProvider.setStatus(BINDING);
            this.waitAndSendViaSocket(PARAM_UPDATE_STATUS, BINDING);

        }

        currentProvider.setCurrentBindsCount(currentProvider.getCurrentBindsCount() + 1);
        currentProvider.getBinds().add(source.getSessionId());
        this.waitAndSendViaSocket(PARAM_UPDATE_SESSIONS, String.valueOf(currentProvider.getCurrentBindsCount()));

        if (spSession.getCurrentServiceProvider().getCurrentBindsCount() == 1) {
            this.currentProvider.setStatus(BOUND);
            this.waitAndSendViaSocket(PARAM_UPDATE_STATUS, BOUND);
            this.executorService.execute(this::handlePendingDeliverSm);
        }

        this.publishBindEvent("BIND", source.getSessionId(), getRemoteHost(source));
    }

    private void closeStateProcessor(Session source) {
        spSession.getCurrentSmppSessions().remove(source);
        if (spSession.getCurrentServiceProvider().getCurrentBindsCount() == 1) {
            this.currentProvider.setStatus(UNBINDING);
            this.waitAndSendViaSocket(PARAM_UPDATE_STATUS, UNBINDING);
        }

        currentProvider.setCurrentBindsCount(currentProvider.getCurrentBindsCount() - 1);
        currentProvider.getBinds().remove(source.getSessionId());

        this.waitAndSendViaSocket(PARAM_UPDATE_SESSIONS, String.valueOf(currentProvider.getCurrentBindsCount()));

        this.publishBindEvent("UNBIND", source.getSessionId(), getRemoteHost(source));

        if (spSession.getCurrentServiceProvider().getCurrentBindsCount() == 0) {
            this.currentProvider.setStatus(STARTED);
            this.waitAndSendViaSocket(PARAM_UPDATE_STATUS, STARTED);
        }
    }

    private void waitAndSendViaSocket(String param, String value) {
        if (notifyWs) {
            this.waitForSessionState();
            this.sendStompMessage(param, value);
        }
    }

    @Generated
    private void waitForSessionState() { // This method is used to wait for sending the next message to the websocket
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            log.error("An error has occurred: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    private void handlePendingDeliverSm() {
        var pendingDeliverSm = spSession.getPendingDlrCache().drainAll();
        if (pendingDeliverSm.isEmpty()) {
            log.debug("The state of networkId {} is BOUND but there are no pending deliver_sm", currentProvider.getNetworkId());
            return;
        }
        var deliverSmEvents = pendingDeliverSm.stream()
                .map(this::castToDeliverSmEvent)
                .filter(Objects::nonNull)
                .toList();

        deliverSmEvents.forEach(deliverSmEvent -> {
            var serverSession = (SMPPServerSession) spSession.getNextRoundRobinSession();
            if (Objects.isNull(serverSession)) {
                log.warn("No active session to send deliver_sm with id {}", deliverSmEvent.getId());
                spSession.getPendingDlrCache().put(deliverSmEvent);
                return;
            }
            StaticMethods.sendDeliverSm(serverSession, deliverSmEvent, currentProvider.isDlrTlvEnabled(), smppGeneralSettings, kafkaTemplate);
        });
    }

    private void publishBindEvent(String eventType, String sessionId, String host) {
        if (!bindNotificationEnabled) {
            return;
        }
        BindEvent bindEvent = BindEvent.builder()
                .eventType(eventType)
                .networkId(currentProvider.getNetworkId())
                .systemId(currentProvider.getSystemId())
                .host(host)
                .sessionId(sessionId)
                .timestamp(System.currentTimeMillis())
                .build();

        kafkaTemplate.send(KafkaTopicsConstants.SMPP_BIND_EVENTS_TOPIC, bindEvent.toString());
        log.info("Published {} event for networkId {} from host {}", eventType, currentProvider.getNetworkId(), host);
    }

    private String getRemoteHost(Session source) {
        if (source instanceof SMPPServerSession serverSession && serverSession.getInetAddress() != null) {
            return serverSession.getInetAddress().getHostAddress();
        }
        return null;
    }

    private MessageEvent castToDeliverSmEvent(String deliverSmRaw) {
        return Converter.stringToObject(deliverSmRaw, MessageEvent.class);
    }
}
