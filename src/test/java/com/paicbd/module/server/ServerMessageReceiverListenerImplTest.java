package com.paicbd.module.server;

import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.utils.ConcatenatedMessageDetails;
import com.paicbd.module.utils.Constants;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import org.jsmpp.bean.GSMSpecificFeature;
import org.jsmpp.bean.SubmitSm;
import org.jsmpp.extra.ProcessRequestException;
import org.jsmpp.session.SMPPServerSession;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import com.paicbd.smsc.utils.RedisManager;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ServerMessageReceiverListenerImplTest {
    @Mock
    AtomicInteger requestCounter;
    @Mock
    SpSession spSession;
    @Mock
    GeneralSettingsCacheConfig generalSettingsCacheConfig;
    @Mock
    MultiPartsHandler multiPartsHandler;
    @Mock
    KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    private RateLimiterUtilManager rateLimiterManager;
    @Mock
    ProxyResponseHandler proxyResponseHandler;

    private static final long PROXY_TIMEOUT = 1000L;

    // Built explicitly in setUp(); the listener has no no-arg constructor for @InjectMocks to use.
    ServerMessageReceiverListenerImpl serverMessageReceiverListener;

    @Mock
    SMPPServerSession smppServerSession;

    @Mock
    RedisManager redisManager;

    @BeforeEach
    void setUp() {
        serverMessageReceiverListener = new ServerMessageReceiverListenerImpl(
                requestCounter,
                spSession,
                generalSettingsCacheConfig,
                multiPartsHandler, kafkaTemplate,
                rateLimiterManager,
                proxyResponseHandler,
                PROXY_TIMEOUT
        );
    }

    @Test
    void onAcceptSubmitSmTwoPartsThenDoItSuccessfully() throws Exception {
        SubmitSm firstSubmitSm = new SubmitSm();
        byte[] firstText = {
                0x7, 0x46, 0x0, 0x0, 0x3, 0x1, 0x2,
                0x1, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
                0x65, 0x20, 0x70, 0x61, 0x72, 0x74, 0x20, 0x31
        };
        firstSubmitSm.setDataCoding((byte) 4);
        firstSubmitSm.setShortMessage(firstText);
        firstSubmitSm.setSourceAddr("1234567890");
        firstSubmitSm.setSourceAddrTon((byte) 0x01);
        firstSubmitSm.setSourceAddrNpi((byte) 0x01);
        firstSubmitSm.setDestAddress("1234567899");
        firstSubmitSm.setDestAddrTon((byte) 0x01);
        firstSubmitSm.setDestAddrNpi((byte) 0x01);
        firstSubmitSm.setEsmClass(GSMSpecificFeature.UDHI.value());

        SubmitSm secondSubmitSm = new SubmitSm();
        byte[] secondText = {
                0x7, 0x46, 0x0, 0x0, 0x3, 0x1, 0x2, 0x2, 0x4d,
                0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x20,
                0x70, 0x61, 0x72, 0x74, 0x20, 0x32
        };
        secondSubmitSm.setDataCoding((byte) 4);
        secondSubmitSm.setShortMessage(secondText);
        secondSubmitSm.setSourceAddr("1234567890");
        secondSubmitSm.setSourceAddrTon((byte) 0x01);
        secondSubmitSm.setSourceAddrNpi((byte) 0x01);
        secondSubmitSm.setDestAddress("1234567899");
        secondSubmitSm.setDestAddrTon((byte) 0x01);
        secondSubmitSm.setDestAddrNpi((byte) 0x01);
        firstSubmitSm.setEsmClass(GSMSpecificFeature.UDHI.value());

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());

        serverMessageReceiverListener = new ServerMessageReceiverListenerImpl(
                requestCounter,
                spSession,
                generalSettingsCacheConfig,
                multiPartsHandler, kafkaTemplate,
                rateLimiterManager,
                proxyResponseHandler,
                PROXY_TIMEOUT
        );

        when(smppServerSession.getInetAddress())
                .thenReturn(java.net.InetAddress.getByName("127.0.0.1"));

        ServiceProvider mockSp = ServiceProvider.builder()
                .systemId("testSP")
                .tps(new AtomicInteger(1))
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .build();

        when(spSession.getCurrentServiceProvider()).thenReturn(mockSp);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);

        serverMessageReceiverListener.onAcceptSubmitSm(firstSubmitSm, smppServerSession);
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));
        serverMessageReceiverListener.onAcceptSubmitSm(secondSubmitSm, smppServerSession);

        verify(requestCounter, times(2)).incrementAndGet();

        ArgumentCaptor<MessageEvent> messageEventCaptor = ArgumentCaptor.forClass(MessageEvent.class);
        verify(multiPartsHandler).messagePartsProcessor(
                messageEventCaptor.capture(),
                any(ConcatenatedMessageDetails.class),
                any(SubmitSm.class),
                eq(KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.HIGH_PRIORITY))
        );

        MessageEvent messageEvent = messageEventCaptor.getValue();
        assertNotNull(messageEvent);
        assertEquals(2, messageEvent.getUdhRaw().size());
        assertEquals(7, messageEvent.getUdhLength());

        verifyNoMoreInteractions(multiPartsHandler);
        verifyNoMoreInteractions(spSession);
    }

    static Stream<String> priorityProvider() {
        return Stream.of(
                GeneralSmscConstants.HIGH_PRIORITY,
                GeneralSmscConstants.MEDIUM_PRIORITY,
                GeneralSmscConstants.LOW_PRIORITY
        );
    }

    @ParameterizedTest
    @MethodSource("priorityProvider")
    void onAcceptSingleSubmitThenDoItSuccessfully(String priority) throws Exception {
        SubmitSm firstSubmitSm = new SubmitSm();
        byte[] firstText = {
                0x2, 0x46, 0x0, 0x4d,
                0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
                0x20, 0x70, 0x61, 0x72, 0x74, 0x20, 0x31
        };
        firstSubmitSm.setDataCoding((byte) 4);
        firstSubmitSm.setShortMessage(firstText);
        firstSubmitSm.setSourceAddr("1234567890");
        firstSubmitSm.setSourceAddrTon((byte) 0x01);
        firstSubmitSm.setSourceAddrNpi((byte) 0x01);
        firstSubmitSm.setDestAddress("1234567899");
        firstSubmitSm.setDestAddrTon((byte) 0x01);
        firstSubmitSm.setDestAddrNpi((byte) 0x01);
        firstSubmitSm.setEsmClass(GSMSpecificFeature.UDHI.value());

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());

        serverMessageReceiverListener = new ServerMessageReceiverListenerImpl(
                requestCounter,
                spSession,
                generalSettingsCacheConfig,
                multiPartsHandler, kafkaTemplate,
                rateLimiterManager,
                proxyResponseHandler,
                PROXY_TIMEOUT
        );

        when(smppServerSession.getInetAddress())
                .thenReturn(java.net.InetAddress.getByName("127.0.0.1"));

        ServiceProvider mockSp = ServiceProvider.builder()
                .systemId("testSP")
                .tps(new AtomicInteger(1))
                .messagePriority(priority)
                .build();

        when(spSession.getCurrentServiceProvider()).thenReturn(mockSp);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);

        serverMessageReceiverListener.onAcceptSubmitSm(firstSubmitSm, smppServerSession);

        verify(requestCounter).incrementAndGet();
        ArgumentCaptor<String> kafkaTopicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> messageEventCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(kafkaTopicCaptor.capture(), messageEventCaptor.capture());
        MessageEvent messageEvent = Converter.stringToObject(messageEventCaptor.getValue(), MessageEvent.class);
        assertNotNull(messageEvent);

        assertEquals(1, messageEvent.getUdhRaw().size());
        assertEquals(2, messageEvent.getUdhLength());
        assertTrue(messageEvent.getCustomParams().get("host").toString()
                .contains("127.0.0.1"));
        switch (messageEvent.getSmscMessagePriority()) {
            case GeneralSmscConstants.HIGH_PRIORITY -> assertEquals(KafkaTopicsConstants.PRE_MESSAGE_HIGH_TOPIC, kafkaTopicCaptor.getValue());
            case GeneralSmscConstants.MEDIUM_PRIORITY -> assertEquals(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC, kafkaTopicCaptor.getValue());
            case GeneralSmscConstants.LOW_PRIORITY -> assertEquals(KafkaTopicsConstants.PRE_MESSAGE_LOW_TOPIC, kafkaTopicCaptor.getValue());
            default -> throw new IllegalStateException("Unexpected value: " + messageEvent.getSmscMessagePriority());
        }
    }

    @Test
    @DisplayName("onAcceptSubmitSm when data coding is invalid")
    void onAcceptSubmitSmWhenDataCodingIsInvalidThenProcessRequestExceptionAndDoNothing() {
        SubmitSm submitSm = new SubmitSm();
        submitSm.setDataCoding((byte) 10); // wrong value

        when(smppServerSession.getSessionId()).thenReturn("session-1234");
        ServiceProvider mockSp = ServiceProvider.builder()
                .systemId("testSP")
                .tps(new AtomicInteger(1))
                .build();

        when(spSession.getCurrentServiceProvider()).thenReturn(mockSp);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);

        assertThrows(ProcessRequestException.class, () -> serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession));

        verify(requestCounter, never()).incrementAndGet();
        verifyNoInteractions(generalSettingsCacheConfig);
        verifyNoInteractions(redisManager);
    }

    @Test
    @DisplayName("onAcceptSubmitSm when spSession has not available credit")
    void onAcceptSubmitSmWhenHasNotAvailableCreditThenProcessRequestExceptionAndDoNothing() {
        SubmitSm submitSm = new SubmitSm();
        submitSm.setDataCoding((byte) 0);

        when(smppServerSession.getSessionId()).thenReturn("session-1234");
        serverMessageReceiverListener = new ServerMessageReceiverListenerImpl(
                requestCounter,
                spSession,
                generalSettingsCacheConfig,
                multiPartsHandler, kafkaTemplate,
                rateLimiterManager,
                proxyResponseHandler,
                PROXY_TIMEOUT
        );
        ServiceProvider mockSp = ServiceProvider.builder()
                .systemId("testSP")
                .tps(new AtomicInteger(1))
                .build();

        when(spSession.getCurrentServiceProvider()).thenReturn(mockSp);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        assertThrows(ProcessRequestException.class, () -> serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession));

        verify(requestCounter, never()).incrementAndGet();
        verifyNoInteractions(generalSettingsCacheConfig);
        verifyNoInteractions(redisManager);
    }

    @Test
    @DisplayName("When rate limiter denies request then return rate limit exceeded")
    void onAcceptSubmitSmWhenRateLimiterExceededThenReturnError() {

        ServiceProvider currentSp = ServiceProvider.builder()
                .networkId(1)
                .systemId("testSP")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status(Constants.BOUND)
                .currentBindsCount(1)
                .enquireLinkPeriod(5000)
                .build();

        when(spSession.getCurrentServiceProvider()).thenReturn(currentSp);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(false);

        SubmitSm submitSm = new SubmitSm();
        assertThrows(ProcessRequestException.class, () -> serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession));
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("Proxy mode: submit_sm waits for downstream confirmation and succeeds")
    void onAcceptSubmitSmWhenProxyAndDownstreamConfirmsThenSuccess() throws Exception {
        SubmitSm submitSm = buildPlainSubmitSm();

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));
        when(spSession.getCurrentServiceProvider()).thenReturn(proxyServiceProvider());
        when(proxyResponseHandler.waitForResponse(anyString(), anyLong()))
                .thenReturn(new UtilsRecords.HttpProxyResponse("id", false, 0, "", null));

        var result = serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession);

        assertNotNull(result);
        // Registers the future before sending downstream so the confirmation cannot be missed.
        verify(proxyResponseHandler).register(anyString());
        verify(requestCounter).incrementAndGet();

        // The message must be queued with the proxy flag enabled so the downstream emits a confirmation.
        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), payloadCaptor.capture());
        MessageEvent sent = Converter.stringToObject(payloadCaptor.getValue(), MessageEvent.class);
        assertTrue(sent.isUseProxy());
    }

    @Test
    @DisplayName("Proxy mode: submit_sm_resp carries the downstream gateway message id when present")
    void onAcceptSubmitSmWhenProxyConfirmsWithGatewayIdThenResponseUsesGatewayId() throws Exception {
        SubmitSm submitSm = buildPlainSubmitSm();
        String gatewayMessageId = "1e153775-5405-42b7-996e-027ca378b526";

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));
        when(spSession.getCurrentServiceProvider()).thenReturn(proxyServiceProvider());
        when(proxyResponseHandler.waitForResponse(anyString(), anyLong()))
                .thenReturn(new UtilsRecords.HttpProxyResponse("id", false, 0, "", gatewayMessageId));

        var result = serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession);

        assertNotNull(result);
        // The ESME must receive the gateway-assigned id, not the SMSC internal correlation id.
        assertEquals(gatewayMessageId, result.getMessageId());
    }

    @Test
    @DisplayName("Proxy mode: submit_sm_resp keeps the internal id when no gateway id is returned")
    void onAcceptSubmitSmWhenProxyConfirmsWithoutGatewayIdThenResponseKeepsInternalId() throws Exception {
        SubmitSm submitSm = buildPlainSubmitSm();

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));
        when(spSession.getCurrentServiceProvider()).thenReturn(proxyServiceProvider());
        when(proxyResponseHandler.waitForResponse(anyString(), anyLong()))
                .thenReturn(new UtilsRecords.HttpProxyResponse("id", false, 0, "", null)); // gatewayMessageId = null

        var result = serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession);

        assertNotNull(result);
        // Falls back to the SMSC internal id (the generated message id), not the literal "id" correlation value.
        assertNotNull(result.getMessageId());
    }

    @Test
    @DisplayName("Proxy mode: timeout (no downstream confirmation) throws ProcessRequestException")
    void onAcceptSubmitSmWhenProxyTimesOutThenProcessRequestException() throws Exception {
        SubmitSm submitSm = buildPlainSubmitSm();

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));
        when(spSession.getCurrentServiceProvider()).thenReturn(proxyServiceProvider());
        when(proxyResponseHandler.waitForResponse(anyString(), anyLong())).thenReturn(null);

        assertThrows(ProcessRequestException.class,
                () -> serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession));

        // The message is still forwarded; only the ESME response is held back and then failed.
        verify(kafkaTemplate).send(anyString(), anyString());
        verify(requestCounter).incrementAndGet();
    }

    @Test
    @DisplayName("Proxy mode: downstream failure throws ProcessRequestException")
    void onAcceptSubmitSmWhenProxyDownstreamFailsThenProcessRequestException() throws Exception {
        SubmitSm submitSm = buildPlainSubmitSm();

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));
        when(spSession.getCurrentServiceProvider()).thenReturn(proxyServiceProvider());
        when(proxyResponseHandler.waitForResponse(anyString(), anyLong()))
                .thenReturn(new UtilsRecords.HttpProxyResponse("id", true, 8, "downstream error", null));

        assertThrows(ProcessRequestException.class,
                () -> serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession));

        verify(kafkaTemplate).send(anyString(), anyString());
    }

    @Test
    @DisplayName("Non-proxy provider does not wait for downstream confirmation")
    void onAcceptSubmitSmWhenNotProxyThenDoesNotWait() throws Exception {
        SubmitSm submitSm = buildPlainSubmitSm();

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));

        ServiceProvider nonProxySp = ServiceProvider.builder()
                .systemId("testSP")
                .tps(new AtomicInteger(1))
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .proxyMode(false)
                .build();
        when(spSession.getCurrentServiceProvider()).thenReturn(nonProxySp);

        var result = serverMessageReceiverListener.onAcceptSubmitSm(submitSm, smppServerSession);

        assertNotNull(result);
        verifyNoInteractions(proxyResponseHandler);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), payloadCaptor.capture());
        MessageEvent sent = Converter.stringToObject(payloadCaptor.getValue(), MessageEvent.class);
        assertFalse(sent.isUseProxy());
    }

    @Test
    @DisplayName("Proxy mode: multipart segments bypass the accumulator and are forwarded independently")
    void onAcceptSubmitSmWhenProxyAndMultipartThenSegmentForwardedWithoutAccumulation() throws Exception {
        SubmitSm udhSegment = new SubmitSm();
        byte[] segmentBytes = {
                0x7, 0x46, 0x0, 0x0, 0x3, 0x1, 0x2,
                0x1, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
                0x65, 0x20, 0x70, 0x61, 0x72, 0x74, 0x20, 0x31
        };
        udhSegment.setDataCoding((byte) 4);
        udhSegment.setShortMessage(segmentBytes);
        udhSegment.setSourceAddr("1234567890");
        udhSegment.setSourceAddrTon((byte) 0x01);
        udhSegment.setSourceAddrNpi((byte) 0x01);
        udhSegment.setDestAddress("1234567899");
        udhSegment.setDestAddrTon((byte) 0x01);
        udhSegment.setDestAddrNpi((byte) 0x01);
        udhSegment.setEsmClass(GSMSpecificFeature.UDHI.value());

        when(spSession.hasAvailableCredit()).thenReturn(true);
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        when(smppServerSession.getInetAddress()).thenReturn(java.net.InetAddress.getByName("127.0.0.1"));
        when(spSession.getCurrentServiceProvider()).thenReturn(proxyServiceProvider());
        when(proxyResponseHandler.waitForResponse(anyString(), anyLong()))
                .thenReturn(new UtilsRecords.HttpProxyResponse("id", false, 0, "", null));

        var result = serverMessageReceiverListener.onAcceptSubmitSm(udhSegment, smppServerSession);

        assertNotNull(result);
        // Option B: the accumulator is never used in proxy mode; each segment goes straight downstream.
        verifyNoInteractions(multiPartsHandler);

        ArgumentCaptor<String> payloadCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), payloadCaptor.capture());
        MessageEvent sent = Converter.stringToObject(payloadCaptor.getValue(), MessageEvent.class);
        assertTrue(sent.isUseProxy());
        // The standalone segment keeps its concatenation metadata so the gateway can reassemble.
        assertEquals("1", sent.getMsgReferenceNumber());
        assertEquals(2, sent.getTotalSegment());
        assertEquals(1, sent.getSegmentSequence());
    }

    private static ServiceProvider proxyServiceProvider() {
        return ServiceProvider.builder()
                .systemId("testSP")
                .tps(new AtomicInteger(1))
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .proxyMode(true)
                .build();
    }

    private static SubmitSm buildPlainSubmitSm() {
        SubmitSm submitSm = new SubmitSm();
        submitSm.setDataCoding((byte) 0);
        submitSm.setShortMessage("Plain proxy message".getBytes());
        submitSm.setSourceAddr("1234567890");
        submitSm.setSourceAddrTon((byte) 0x01);
        submitSm.setSourceAddrNpi((byte) 0x01);
        submitSm.setDestAddress("1234567899");
        submitSm.setDestAddrTon((byte) 0x01);
        submitSm.setDestAddrNpi((byte) 0x01);
        submitSm.setEsmClass((byte) 0x00);
        return submitSm;
    }

    private static GeneralSettings getGeneralSettings() {
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
}
