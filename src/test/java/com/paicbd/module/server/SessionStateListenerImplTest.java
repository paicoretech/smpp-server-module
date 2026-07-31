package com.paicbd.module.server;

import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.SMPPServerSession;
import org.jsmpp.session.Session;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.paicbd.module.utils.Constants.BINDING;
import static com.paicbd.module.utils.Constants.BOUND;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_SESSIONS;
import static com.paicbd.module.utils.Constants.PARAM_UPDATE_STATUS;
import static com.paicbd.module.utils.Constants.STARTED;
import static com.paicbd.module.utils.Constants.STOPPED;
import static com.paicbd.module.utils.Constants.TYPE;
import static com.paicbd.module.utils.Constants.UNBINDING;
import static com.paicbd.module.utils.Constants.WEBSOCKET_STATUS_ENDPOINT;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SessionStateListenerImplTest {

    @Mock
    StompSession stompSession;

    @Mock
    RedisManager redisManager;

    @Mock
    GeneralSettings smppGeneralSettings;

    @Mock
    Session sessionMock;

    @Mock
    SMPPServerSession serverSession;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    AppProperties appProperties;

    SessionStateListenerImpl sessionStateListener;

    @Test
    @DisplayName("onStateChange when current service provider is BOUND and new session state is CLOSED")
    void onStateChangeWhenCurrentStateIsBOUNDAndNewStateIsCLOSEDThenDoItSuccessfully() {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status(BOUND)
                .enquireLinkPeriod(5000)
                .currentBindsCount(1).dlrTlvEnabled(true)
                .build();

        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        realSpSession.getCurrentSmppSessions().add(sessionMock);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, stompSession, redisManager, smppGeneralSettings, kafkaTemplate, false);

        assertEquals(1, spSessionMapSpy.size());
        assertEquals(1, spSessionSpy.getCurrentServiceProvider().getCurrentBindsCount());
        assertEquals(1, spSessionSpy.getCurrentSmppSessions().size());
        assertEquals(BOUND, spSessionSpy.getCurrentServiceProvider().getStatus());

        sessionStateListener.onStateChange(SessionState.UNBOUND, SessionState.BOUND_TRX, sessionMock);
        sessionStateListener.onStateChange(SessionState.CLOSED, SessionState.UNBOUND, sessionMock);

        ServiceProvider closedServiceProvider = spSessionSpy.getCurrentServiceProvider();
        assertEquals(0, closedServiceProvider.getCurrentBindsCount());
        assertEquals(STARTED, closedServiceProvider.getStatus());
        assertTrue(closedServiceProvider.getBinds().isEmpty());
        assertTrue(spSessionSpy.getCurrentSmppSessions().isEmpty());

        verify(redisManager).hset("service_providers", String.valueOf(closedServiceProvider.getNetworkId()), closedServiceProvider.toString());

        String message = String.format("%s,%s,%s,%s", TYPE, closedServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, UNBINDING);
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, closedServiceProvider.getNetworkId(), PARAM_UPDATE_SESSIONS, "0");
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, closedServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, STARTED);
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);
    }

    @Test
    @DisplayName("onStateChange when service provider has more than one open bind and a session is CLOSED the count decrements correctly")
    void onStateChangeWhenCurrentServiceProviderHasMoreThanOneOpenBindsAndNewStateIsCLOSEDThenDoItSuccessfully() {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>(Arrays.asList("id-1234", "id-5678")))
                .enabled(1)
                .status(BOUND)
                .enquireLinkPeriod(5000)
                .currentBindsCount(2).dlrTlvEnabled(true)
                .build();

        Session secondSessionMock = mock(Session.class);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        realSpSession.getCurrentSmppSessions().add(sessionMock);
        realSpSession.getCurrentSmppSessions().add(secondSessionMock);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, null, redisManager, smppGeneralSettings, kafkaTemplate, false);

        assertEquals(2, spSessionSpy.getCurrentServiceProvider().getCurrentBindsCount());
        assertEquals(2, spSessionSpy.getCurrentSmppSessions().size());

        when(sessionMock.getSessionId()).thenReturn("id-1234");
        sessionStateListener.onStateChange(SessionState.CLOSED, SessionState.BOUND_TRX, sessionMock);

        ServiceProvider closedServiceProvider = spSessionSpy.getCurrentServiceProvider();
        assertEquals(1, closedServiceProvider.getCurrentBindsCount());
        assertEquals(BOUND, closedServiceProvider.getStatus());
        assertEquals(1, closedServiceProvider.getBinds().size());
        assertEquals(1, spSessionSpy.getCurrentSmppSessions().size());

        verify(redisManager).hset("service_providers", String.valueOf(closedServiceProvider.getNetworkId()), closedServiceProvider.toString());
        verifyNoMoreInteractions(stompSession);
    }

    @Test
    @DisplayName("onStateChange when opening new bind drains and sends pending deliver_sm events from the cache")
    void onStateChangeWhenOpenNewConnectionAndSendingPendingDeliverSmThenDoItSuccessfully() throws ResponseTimeoutException, PDUException, IOException, InvalidResponseException, NegativeResponseException {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status(STARTED)
                .enquireLinkPeriod(5000)
                .currentBindsCount(0).dlrTlvEnabled(true)
                .build();

        MessageEvent deliverSmEvent = getDeliverSmEvent(serviceProviderMock.getNetworkId());

        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        realSpSession.getPendingDlrCache().put(deliverSmEvent);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, stompSession, redisManager, getGeneralSettings(), kafkaTemplate, false);

        assertTrue(spSessionSpy.getCurrentSmppSessions().isEmpty());
        assertEquals(0, spSessionSpy.getCurrentServiceProvider().getCurrentBindsCount());
        assertEquals(STARTED, spSessionSpy.getCurrentServiceProvider().getStatus());

        when(sessionMock.getSessionId()).thenReturn("id-12345");
        when(spSessionSpy.getNextRoundRobinSession()).thenReturn(serverSession);
        sessionStateListener.onStateChange(SessionState.BOUND_RX, SessionState.UNBOUND, sessionMock);
        toSleep();

        ServiceProvider boundServiceProvider = spSessionSpy.getCurrentServiceProvider();
        assertEquals(1, boundServiceProvider.getCurrentBindsCount());
        assertEquals(BOUND, boundServiceProvider.getStatus());
        assertFalse(boundServiceProvider.getBinds().isEmpty());
        assertTrue(boundServiceProvider.getBinds().contains("id-12345"));
        assertEquals(1, spSessionSpy.getCurrentSmppSessions().size());

        verify(redisManager).hset("service_providers", String.valueOf(boundServiceProvider.getNetworkId()), boundServiceProvider.toString());

        String message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BINDING);
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_SESSIONS, "1");
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BOUND);
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        verifyDeliverShortMessage(deliverSmEvent);
    }

    @Test
    @DisplayName("onStateChange when opening new bind and there are no pending deliver_sm events in the cache")
    void onStateChangeWhenOpenNewConnectionAndThereAreNoDeliverySmEventThenDoItSuccessfully() {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status(STARTED)
                .enquireLinkPeriod(5000)
                .currentBindsCount(0).dlrTlvEnabled(true)
                .build();

        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, stompSession, redisManager, getGeneralSettings(), kafkaTemplate, false);

        assertTrue(spSessionSpy.getCurrentSmppSessions().isEmpty());
        assertEquals(STARTED, spSessionSpy.getCurrentServiceProvider().getStatus());

        when(sessionMock.getSessionId()).thenReturn("id-12345");
        sessionStateListener.onStateChange(SessionState.BOUND_RX, SessionState.UNBOUND, sessionMock);
        toSleep();

        ServiceProvider boundServiceProvider = spSessionSpy.getCurrentServiceProvider();
        assertEquals(1, boundServiceProvider.getCurrentBindsCount());
        assertEquals(BOUND, boundServiceProvider.getStatus());
        assertTrue(boundServiceProvider.getBinds().contains("id-12345"));
        assertEquals(1, spSessionSpy.getCurrentSmppSessions().size());

        verify(redisManager).hset("service_providers", String.valueOf(boundServiceProvider.getNetworkId()), boundServiceProvider.toString());

        String message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BINDING);
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_SESSIONS, "1");
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BOUND);
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        verifyNoMoreInteractions(serverSession);
    }

    @Test
    @DisplayName("onStateChange when there are pending deliver_sm events and no active session then events are re-queued in the cache")
    void onStateChangeWhenOpenNewConnectionAndNoServerSessionThenPendingEventsAreRequeued() {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status(STARTED)
                .enquireLinkPeriod(5000)
                .currentBindsCount(0).dlrTlvEnabled(true)
                .build();

        MessageEvent deliverSmEvent = getDeliverSmEvent(serviceProviderMock.getNetworkId());

        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        realSpSession.getPendingDlrCache().put(deliverSmEvent);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, stompSession, redisManager, getGeneralSettings(), kafkaTemplate, false);

        when(sessionMock.getSessionId()).thenReturn("id-12345");
        when(spSessionSpy.getNextRoundRobinSession()).thenReturn(null);
        sessionStateListener.onStateChange(SessionState.BOUND_RX, SessionState.UNBOUND, sessionMock);
        toSleep();

        ServiceProvider boundServiceProvider = spSessionSpy.getCurrentServiceProvider();
        assertEquals(1, boundServiceProvider.getCurrentBindsCount());
        assertEquals(BOUND, boundServiceProvider.getStatus());
        assertTrue(boundServiceProvider.getBinds().contains("id-12345"));

        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT,
                String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BINDING));
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT,
                String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_SESSIONS, "1"));
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT,
                String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BOUND));

        verifyNoMoreInteractions(serverSession);

        var requeued = spSessionSpy.getPendingDlrCache().drainAll();
        assertEquals(1, requeued.size());
        assertTrue(requeued.iterator().next().contains(deliverSmEvent.getMessageId()));
    }

    @Test
    @DisplayName("onStateChange when there are already open binds a new BOUND_RX state adds to the bind count without re-triggering BINDING status")
    void onStateChangeWhenBindsAreOpenAndNewStateIsBOUNDRXThenAddNewBinds() {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>(java.util.List.of("id-1234")))
                .enabled(1)
                .status(BOUND)
                .enquireLinkPeriod(5000)
                .currentBindsCount(1).dlrTlvEnabled(true)
                .build();

        Session secondSessionMock = mock(Session.class);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        realSpSession.getCurrentSmppSessions().add(secondSessionMock);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, stompSession, redisManager, smppGeneralSettings, kafkaTemplate, false);

        assertEquals(1, spSessionSpy.getCurrentSmppSessions().size());
        assertEquals(1, spSessionSpy.getCurrentServiceProvider().getCurrentBindsCount());
        assertEquals(BOUND, spSessionSpy.getCurrentServiceProvider().getStatus());

        when(sessionMock.getSessionId()).thenReturn("id-5678");
        sessionStateListener.onStateChange(SessionState.BOUND_RX, SessionState.BOUND_RX, sessionMock);

        ServiceProvider boundServiceProvider = spSessionSpy.getCurrentServiceProvider();
        assertEquals(2, boundServiceProvider.getCurrentBindsCount());
        assertEquals(BOUND, boundServiceProvider.getStatus());
        assertEquals(2, boundServiceProvider.getBinds().size());
        assertTrue(boundServiceProvider.getBinds().contains("id-5678"));
        assertEquals(2, spSessionSpy.getCurrentSmppSessions().size());

        verify(redisManager).hset("service_providers", String.valueOf(boundServiceProvider.getNetworkId()), boundServiceProvider.toString());

        String message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BINDING);
        verify(stompSession, never()).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_SESSIONS, "2");
        verify(stompSession).send(WEBSOCKET_STATUS_ENDPOINT, message);

        message = String.format("%s,%s,%s,%s", TYPE, boundServiceProvider.getNetworkId(), PARAM_UPDATE_STATUS, BOUND);
        verify(stompSession, never()).send(WEBSOCKET_STATUS_ENDPOINT, message);
    }

    @Test
    @DisplayName("onStateChange when service provider is STOPPED all state changes are ignored")
    void onStateChangeWhenSTOPPEDCurrentServiceProviderAndStompSessionIsnullThenDoNothing() {
        ServiceProvider serviceProviderMock = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(0)
                .status(STOPPED)
                .enquireLinkPeriod(5000)
                .currentBindsCount(0).dlrTlvEnabled(true)
                .build();

        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        SpSession realSpSession = new SpSession(redisManager, serviceProviderMock, appProperties, kafkaTemplate);
        SpSession spSessionSpy = spy(realSpSession);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        realSpSessionMap.put(serviceProviderMock.getNetworkId(), spSessionSpy);
        ConcurrentMap<Integer, SpSession> spSessionMapSpy = spy(realSpSessionMap);

        sessionStateListener = new SessionStateListenerImpl(1, spSessionMapSpy, stompSession, redisManager, smppGeneralSettings, kafkaTemplate, false);

        assertEquals(1, spSessionMapSpy.size());
        assertEquals(STOPPED, spSessionSpy.getCurrentServiceProvider().getStatus());

        sessionStateListener.onStateChange(SessionState.BOUND_TRX, SessionState.CLOSED, sessionMock);

        assertEquals(1, spSessionMapSpy.size());
        assertEquals(STOPPED, spSessionSpy.getCurrentServiceProvider().getStatus());

        verifyNoMoreInteractions(stompSession);
        verifyNoMoreInteractions(redisManager);
    }

    private void verifyDeliverShortMessage(MessageEvent deliverSmEvent) throws ResponseTimeoutException, PDUException, IOException, InvalidResponseException, NegativeResponseException {
        ArgumentCaptor<String> sourceAddrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeOfNumber> sourceAddrTonCaptor = ArgumentCaptor.forClass(TypeOfNumber.class);
        ArgumentCaptor<NumberingPlanIndicator> sourceAddrNpiCaptor = ArgumentCaptor.forClass(NumberingPlanIndicator.class);
        ArgumentCaptor<String> destAddrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeOfNumber> destAddrTonCaptor = ArgumentCaptor.forClass(TypeOfNumber.class);
        ArgumentCaptor<NumberingPlanIndicator> destAddrNpiCaptor = ArgumentCaptor.forClass(NumberingPlanIndicator.class);
        ArgumentCaptor<DataCoding> dataCodingCaptor = ArgumentCaptor.forClass(DataCoding.class);
        ArgumentCaptor<byte[]> encodedShortMessageCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<String> serviceTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ESMClass> esmClassArgumentCaptor = ArgumentCaptor.forClass(ESMClass.class);
        ArgumentCaptor<Byte> protocolIdCaptor = ArgumentCaptor.forClass(Byte.class);
        ArgumentCaptor<Byte> priorityFlagCaptor = ArgumentCaptor.forClass(Byte.class);
        ArgumentCaptor<RegisteredDelivery> registeredDeliveryCaptor = ArgumentCaptor.forClass(RegisteredDelivery.class);
        ArgumentCaptor<OptionalParameter[]> optionalParameterCaptor = ArgumentCaptor.forClass(OptionalParameter[].class);
        verify(serverSession).deliverShortMessage(
                serviceTypeCaptor.capture(),
                sourceAddrTonCaptor.capture(),
                sourceAddrNpiCaptor.capture(),
                sourceAddrCaptor.capture(),
                destAddrTonCaptor.capture(),
                destAddrNpiCaptor.capture(),
                destAddrCaptor.capture(),
                esmClassArgumentCaptor.capture(),
                protocolIdCaptor.capture(),
                priorityFlagCaptor.capture(),
                registeredDeliveryCaptor.capture(),
                dataCodingCaptor.capture(),
                encodedShortMessageCaptor.capture(),
                optionalParameterCaptor.capture()
        );
        assertEquals(deliverSmEvent.getSourceAddr(), sourceAddrCaptor.getValue());
        assertEquals(UtilsEnum.getTypeOfNumber(deliverSmEvent.getSourceAddrTon()), sourceAddrTonCaptor.getValue());
        assertEquals(deliverSmEvent.getDestinationAddr(), destAddrCaptor.getValue());
        assertEquals(UtilsEnum.getTypeOfNumber(deliverSmEvent.getDestAddrTon()), destAddrTonCaptor.getValue());
        assertEquals(UtilsEnum.getNumberingPlanIndicator(deliverSmEvent.getDestAddrNpi()), destAddrNpiCaptor.getValue());
        assertEquals(UtilsEnum.getNumberingPlanIndicator(deliverSmEvent.getSourceAddrNpi()), sourceAddrNpiCaptor.getValue());
        assertEquals(EncodingUtils.getDataCoding(deliverSmEvent.getDataCoding()), dataCodingCaptor.getValue());
        assertArrayEquals(deliverSmEvent.getDelReceipt().getBytes(), encodedShortMessageCaptor.getValue());
    }

    private GeneralSettings getGeneralSettings() {
        return GeneralSettings.builder()
                .id(1)
                .validityPeriod(60)
                .maxValidityPeriod(240)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(3)
                .encodingGsm7(0)
                .encodingUcs2(2)
                .build();
    }

    private MessageEvent getDeliverSmEvent(int destNetworkId) {
        return MessageEvent.builder()
                .id("1719421854353-11028072268459")
                .messageId("1719421854353-11028072268459")
                .systemId("systemId123")
                .commandStatus(0)
                .sequenceNumber(1)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50510201020")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .esmClass(0)
                .validityPeriod(60)
                .registeredDelivery(1)
                .dataCoding(0)
                .smDefaultMsgId(0)
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(2)
                .destNetworkType("GW")
                .destProtocol("SMPP")
                .destNetworkId(destNetworkId)
                .routingId(1)
                .isRetry(false)
                .isLastRetry(false)
                .isNetworkNotifyError(false)
                .dueDelay(0)
                .checkSriResponse(false)
                .isDlr(false)
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .optionalParameters(null)
                .build();
    }

    private static void toSleep() {
        await().atMost(ONE_SECOND).until(() -> true);
    }
}
