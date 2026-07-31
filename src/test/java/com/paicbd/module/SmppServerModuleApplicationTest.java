package com.paicbd.module;

import com.paicbd.module.components.DeliverSmQueueConsumer;
import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.e2e.SmppClientMock;
import com.paicbd.module.server.CustomSmppServer;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.SmppServerConfig;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.ws.SocketSession;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.SubmitSmResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.paicbd.module.utils.Constants.ALL_BIND_TYPES;
import static com.paicbd.module.utils.Constants.STARTED_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED_STATUS;
import static java.lang.Thread.startVirtualThread;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

@Slf4j
@ExtendWith(MockitoExtension.class)
class SmppServerModuleApplicationTest {
    final String host = "127.0.0.1";
    final int port = getRandomLocalPort();
    final ExecutorService executor = Executors.newSingleThreadExecutor();
    final List<ServiceProvider> serviceProviders = new ArrayList<>();

    @Mock
    RedisManager redisManager;

    @Mock
    SocketSession socketSession;

    @Mock
    GeneralSettingsCacheConfig generalSettingsCacheConfig;

    @Mock
    ScyllaManager scyllaManager;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    CustomSmppServer smppServer;

    DeliverSmQueueConsumer deliverSmQueueConsumer;

    @Mock
    private RateLimiterUtilManager rateLimiterManager;

    @Mock
    private AppProperties appProperties;
    @Mock
    private ProxyResponseHandler proxyResponseHandler;

    SMPPSession smppSession;
    SMPPSession smppSessionReceiver;
    SmppClientMock smppClient;
    ConcurrentMap<Integer, SpSession> spSessionMapSpy;

    ServiceProvider serviceProviderWithoutCredit;
    ServiceProvider stoppedServiceProvider;
    ServiceProvider invalidBindTypeServiceProvider;
    ServiceProvider invalidSmppServerIdServiceProvider;
    ServiceProvider invalidPasswordServiceProvider;
    ServiceProvider invalidInterfaceVersionServiceProvider;
    ServiceProvider ifAnyInterfaceVersionServiceProvider;
    ServiceProvider nullInterfaceVersionServiceProvider;
    ServiceProvider invalidAddressRangeServiceProvider;
    ServiceProvider invalidSystemTypeServiceProvider;
    ServiceProvider okServiceProviderBindTransmitter;

    @BeforeEach
    void setUp() {
        setDifferentServiceProvider();

        serviceProviders.add(getServiceProviderByBindTypes(ALL_BIND_TYPES));
        serviceProviders.add(serviceProviderWithoutCredit);
        serviceProviders.add(stoppedServiceProvider);
        serviceProviders.add(invalidBindTypeServiceProvider);
        serviceProviders.add(invalidSmppServerIdServiceProvider);
        serviceProviders.add(invalidPasswordServiceProvider);
        serviceProviders.add(invalidInterfaceVersionServiceProvider);
        serviceProviders.add(ifAnyInterfaceVersionServiceProvider);
        serviceProviders.add(nullInterfaceVersionServiceProvider);
        serviceProviders.add(okServiceProviderBindTransmitter);

        startSmppServer();
        toSleep(1);
        smppClient = new SmppClientMock(host, port);
        smppSession = smppClient.createAndBindSmppSession(getServiceProviderByBindTypes("TRANSMITTER"));
        assertTrue(smppSession.getSessionState().isBound());
        smppSessionReceiver = smppClient.createAndBindSmppSession(getServiceProviderByBindTypes("RECEIVER"));
        assertTrue(smppSessionReceiver.getSessionState().isBound());
        when(rateLimiterManager.tryConsume(anyString(), anyInt())).thenReturn(true);
    }

    @AfterEach
    void tearDown() {
        smppSession.unbindAndClose();
        executor.shutdownNow();
        smppServer.stop();
    }

    @Test
    @DisplayName("Start SmppServer and open connection when send submit and receive deliverSm then do it successfully")
    void startSmppServerAndOpenConnectionWhenSendSubmitAndReceiveDeliverSmThenDoItSuccessfully() {
        log.info("Start testing SmppServerModuleApplicationTest");
        AtomicInteger messageCount = new AtomicInteger();
        executeOpenConnectionsTests();
        toSleep(2);
        messageEventProvider().forEach(messageEvent -> {
            sendSubmitSmWithAndWithoutOptionalParameterReturnsExpectedDeliverSm(messageEvent);
            toSleep(2);
            messageCount.getAndIncrement();
            if (messageCount.get() == 3) {
                smppSessionReceiver.unbindAndClose();
            }
        });
    }

    private void startSmppServer() {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ConcurrentMap<Integer, SpSession> realSpSessionMap = new ConcurrentHashMap<>();
        Set<ServiceProvider> realProviders = new HashSet<>();

        for (ServiceProvider serviceProvider : serviceProviders) {
            SpSession realSpSession = new SpSession(redisManager, serviceProvider, appProperties, kafkaTemplate);
            realSpSession.updateCurrentServiceProvider(serviceProvider);
            SpSession realSpSessionSpy = spy(realSpSession);
            realProviders.add(serviceProvider);
            realSpSessionMap.put(serviceProvider.getNetworkId(), realSpSessionSpy);
        }

        spSessionMapSpy = spy(realSpSessionMap);
        when(socketSession.getStompSession()).thenReturn(null);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());

        StompSession stompSession = mock(StompSession.class);
        when(socketSession.getStompSession()).thenReturn(stompSession);

        SmppServerConfig config = SmppServerConfig.builder()
                .id(1).name("MAIN-SMPP-SERVER").ip(host).port(port)
                .transactionTimer(2000).waitForBind(2000).processorDegree(15).queueCapacity(1000)
                .status(STOPPED_STATUS).enabled(1)
                .build();
        smppServer = new CustomSmppServer(redisManager, socketSession, realProviders, config, spSessionMapSpy,
                generalSettingsCacheConfig, scyllaManager, kafkaTemplate, rateLimiterManager, appProperties, proxyResponseHandler);
        startVirtualThread(smppServer);
        toSleep(2);

        assertEquals(STARTED_STATUS, config.getStatus());
        when(this.generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(getGeneralSettings());
        this.deliverSmQueueConsumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMapSpy, generalSettingsCacheConfig);
    }

    // Sending single submitSm and get deliverSm response
    private void sendSubmitSmWithAndWithoutOptionalParameterReturnsExpectedDeliverSm(MessageEvent submitSmEvent) {
        // sending message
        SubmitSmResult submitSmResult = smppClient.sendSubmit(submitSmEvent, smppSession);
        assertNotNull(submitSmResult.getMessageId());
        toSleep(2);

        // sending deliver sm
        if (submitSmEvent.getRegisteredDelivery() == 1) {
            this.deliverSmQueueConsumer.handleDeliverySms(List.of(getDeliverSM(submitSmResult.getMessageId()).toString()));
        }
    }

    private void executeOpenConnectionsTests() {
        startVirtualThread(this::openingConnectionWhenInvalidPasswordThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenMaxBindExceededThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenServiceProviderDoesNotExistThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenServiceProviderIsStatusStoppedThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenServiceProviderHasNotCreditThenSubmitIsInvalid);
        startVirtualThread(this::openingConnectionWithBindTypeInvalidThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenInvalidSmppServerIdThenSpSessionIsNull);

        startVirtualThread(this::openingConnectionWhenInvalidInterfaceVersionThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenProviderInterfaceVersionIsIfAnyThenSpSessionIsNotNull);
        startVirtualThread(this::openingConnectionWhenProviderInterfaceVersionIsNullThenSpSessionIsNotNull);
        startVirtualThread(this::openingConnectionWhenInvalidAddressRangeThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWhenInvalidSystemTypeThenSpSessionIsNull);
        startVirtualThread(this::openingConnectionWithAllBindTypeThenSpSessionIsNotNull);
    }

    private void openingConnectionWhenInvalidPasswordThenSpSessionIsNull() {
        ServiceProvider okServiceProviderClone = ServiceProvider.builder()
                .networkId(1)
                .systemId("successSmppSp")
                .password("invalidPassword")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("4455")
                .interfaceVersion("IF_33")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .build();

        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(okServiceProviderClone);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenMaxBindExceededThenSpSessionIsNull() {
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(getServiceProviderByBindTypes("RECEIVER"));
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenServiceProviderDoesNotExistThenSpSessionIsNull() {
        ServiceProvider invalidServiceprovider = ServiceProvider.builder()
                .networkId(2)
                .systemId("invalidServiceProvider")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .build();

        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(invalidServiceprovider);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenServiceProviderIsStatusStoppedThenSpSessionIsNull() {
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(stoppedServiceProvider);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenServiceProviderHasNotCreditThenSubmitIsInvalid() {
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(serviceProviderWithoutCredit);
        assertNotNull(newMockSmppSession);

        // sending message
        SubmitSmResult submitSmResult = smppClient.sendSubmit(getSingleSubmitSmEvent(), newMockSmppSession);
        assertNull(submitSmResult);
    }

    // invalidBindTypeServiceProviderClone in smppServer session is bind type TRANSMITTER
    private void openingConnectionWithBindTypeInvalidThenSpSessionIsNull() {
        ServiceProvider invalidBindTypeServiceProviderClone = ServiceProvider.builder()
                .networkId(4)
                .systemId("invalidBindSP")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSMITTER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .build();

        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(invalidBindTypeServiceProviderClone);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenInvalidSmppServerIdThenSpSessionIsNull() {
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(invalidSmppServerIdServiceProvider);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenInvalidInterfaceVersionThenSpSessionIsNull() {
        ServiceProvider bindRequestProvider = ServiceProvider.builder()
                .networkId(invalidInterfaceVersionServiceProvider.getNetworkId())
                .systemId(invalidInterfaceVersionServiceProvider.getSystemId())
                .password(invalidInterfaceVersionServiceProvider.getPassword())
                .protocol(invalidInterfaceVersionServiceProvider.getProtocol())
                .binds(new ArrayList<>())
                .enabled(invalidInterfaceVersionServiceProvider.getEnabled())
                .status(invalidInterfaceVersionServiceProvider.getStatus())
                .enquireLinkPeriod(invalidInterfaceVersionServiceProvider.getEnquireLinkPeriod())
                .pduTimeout(invalidInterfaceVersionServiceProvider.getPduTimeout())
                .bindType(invalidInterfaceVersionServiceProvider.getBindType())
                .systemType(invalidInterfaceVersionServiceProvider.getSystemType())
                .addressNpi(invalidInterfaceVersionServiceProvider.getAddressNpi())
                .addressTon(invalidInterfaceVersionServiceProvider.getAddressTon())
                .addressRange(invalidInterfaceVersionServiceProvider.getAddressRange())
                .interfaceVersion("IF_50")
                .currentBindsCount(invalidInterfaceVersionServiceProvider.getCurrentBindsCount())
                .hasAvailableCredit(invalidInterfaceVersionServiceProvider.getHasAvailableCredit())
                .maxBinds(invalidInterfaceVersionServiceProvider.getMaxBinds())
                .smppServerId(invalidInterfaceVersionServiceProvider.getSmppServerId())
                .build();

        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(bindRequestProvider);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenProviderInterfaceVersionIsIfAnyThenSpSessionIsNotNull() {
        ServiceProvider bindRequestProvider = ServiceProvider.builder()
                .networkId(ifAnyInterfaceVersionServiceProvider.getNetworkId())
                .systemId(ifAnyInterfaceVersionServiceProvider.getSystemId())
                .password(ifAnyInterfaceVersionServiceProvider.getPassword())
                .protocol(ifAnyInterfaceVersionServiceProvider.getProtocol())
                .binds(new ArrayList<>())
                .enabled(ifAnyInterfaceVersionServiceProvider.getEnabled())
                .status(ifAnyInterfaceVersionServiceProvider.getStatus())
                .enquireLinkPeriod(ifAnyInterfaceVersionServiceProvider.getEnquireLinkPeriod())
                .pduTimeout(ifAnyInterfaceVersionServiceProvider.getPduTimeout())
                .bindType(ifAnyInterfaceVersionServiceProvider.getBindType())
                .systemType(ifAnyInterfaceVersionServiceProvider.getSystemType())
                .addressNpi(ifAnyInterfaceVersionServiceProvider.getAddressNpi())
                .addressTon(ifAnyInterfaceVersionServiceProvider.getAddressTon())
                .addressRange(ifAnyInterfaceVersionServiceProvider.getAddressRange())
                .interfaceVersion("IF_50")
                .currentBindsCount(ifAnyInterfaceVersionServiceProvider.getCurrentBindsCount())
                .hasAvailableCredit(ifAnyInterfaceVersionServiceProvider.getHasAvailableCredit())
                .maxBinds(ifAnyInterfaceVersionServiceProvider.getMaxBinds())
                .smppServerId(ifAnyInterfaceVersionServiceProvider.getSmppServerId())
                .build();

        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(bindRequestProvider);
        assertNotNull(newMockSmppSession);
    }

    private void openingConnectionWhenProviderInterfaceVersionIsNullThenSpSessionIsNotNull() {
        ServiceProvider bindRequestProvider = ServiceProvider.builder()
                .networkId(nullInterfaceVersionServiceProvider.getNetworkId())
                .systemId(nullInterfaceVersionServiceProvider.getSystemId())
                .password(nullInterfaceVersionServiceProvider.getPassword())
                .protocol(nullInterfaceVersionServiceProvider.getProtocol())
                .binds(new ArrayList<>())
                .enabled(nullInterfaceVersionServiceProvider.getEnabled())
                .status(nullInterfaceVersionServiceProvider.getStatus())
                .enquireLinkPeriod(nullInterfaceVersionServiceProvider.getEnquireLinkPeriod())
                .pduTimeout(nullInterfaceVersionServiceProvider.getPduTimeout())
                .bindType(nullInterfaceVersionServiceProvider.getBindType())
                .systemType(nullInterfaceVersionServiceProvider.getSystemType())
                .addressNpi(nullInterfaceVersionServiceProvider.getAddressNpi())
                .addressTon(nullInterfaceVersionServiceProvider.getAddressTon())
                .addressRange(nullInterfaceVersionServiceProvider.getAddressRange())
                .interfaceVersion("IF_50")
                .currentBindsCount(nullInterfaceVersionServiceProvider.getCurrentBindsCount())
                .hasAvailableCredit(nullInterfaceVersionServiceProvider.getHasAvailableCredit())
                .maxBinds(nullInterfaceVersionServiceProvider.getMaxBinds())
                .smppServerId(nullInterfaceVersionServiceProvider.getSmppServerId())
                .build();

        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(bindRequestProvider);
        assertNotNull(newMockSmppSession);
    }

    private void openingConnectionWhenInvalidAddressRangeThenSpSessionIsNull() {
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(invalidAddressRangeServiceProvider);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWhenInvalidSystemTypeThenSpSessionIsNull() {
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(invalidSystemTypeServiceProvider);
        assertNull(newMockSmppSession);
    }

    private void openingConnectionWithAllBindTypeThenSpSessionIsNotNull() {
        ServiceProvider okServiceProviderWithAllBindTypesClone = ServiceProvider.builder()
                .networkId(9)
                .systemId("transmitterSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSMITTER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("4455")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();
        SMPPSession newMockSmppSession = smppClient.createAndBindSmppSession(okServiceProviderWithAllBindTypesClone);
        assertNotNull(newMockSmppSession);
    }

    private MessageEvent getSingleSubmitSmEvent() {
        return MessageEvent.builder()
                .systemId(serviceProviderWithoutCredit.getSystemId())
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("1234567890")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("1234567890")
                .esmClass(0)
                .validityPeriod(0)
                .registeredDelivery(0)
                .dataCoding(0)
                .shortMessage("Origin is a service provider without credit")
                .isDlr(false)
                .build();
    }

    private Stream<MessageEvent> messageEventProvider() {
        return Stream.of(
                MessageEvent.builder()
                        .systemId("testSP")
                        .sourceAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddr("1234567890")
                        .destAddrTon(1)
                        .destAddrNpi(1)
                        .destinationAddr("1234567890")
                        .esmClass(0)
                        .validityPeriod(0)
                        .registeredDelivery(1)
                        .dataCoding(0)
                        .shortMessage("This is a single submitSm test with data coding GSM7")
                        .isDlr(false)
                        .build(),
                MessageEvent.builder()
                        .systemId("testSP")
                        .sourceAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddr("1234567890")
                        .destAddrTon(1)
                        .destAddrNpi(1)
                        .destinationAddr("1234567890")
                        .esmClass(0)
                        .validityPeriod(0)
                        .registeredDelivery(1)
                        .dataCoding(3)
                        .shortMessage("This is a single submitSm test with data coding ISO88591")
                        .isDlr(false)
                        .build(),
                MessageEvent.builder()
                        .systemId("testSP")
                        .sourceAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddr("1234567890")
                        .destAddrTon(1)
                        .destAddrNpi(1)
                        .destinationAddr("1234567890")
                        .esmClass(0)
                        .validityPeriod(0)
                        .registeredDelivery(1)
                        .dataCoding(8) // 8 is unicode data coding
                        .shortMessage("This is a multipart test with optionalParameters and data coding unicode - part 1")
                        .isDlr(false)
                        .msgReferenceNumber("1")
                        .segmentSequence(1)
                        .totalSegment(2)
                        .optionalParameters(getOptionalParameters("1"))
                        .build(),
                MessageEvent.builder()
                        .systemId("testSP")
                        .sourceAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddr("1234567890")
                        .destAddrTon(1)
                        .destAddrNpi(1)
                        .destinationAddr("1234567890")
                        .esmClass(0)
                        .validityPeriod(0)
                        .registeredDelivery(1)
                        .dataCoding(8) // 8 is unicode data coding
                        .shortMessage("This is a multipart test with optionalParameters and data coding unicode - part 2")
                        .isDlr(false)
                        .msgReferenceNumber("1")
                        .segmentSequence(2)
                        .totalSegment(2)
                        .optionalParameters(getOptionalParameters("2"))
                        .build(),
                MessageEvent.builder()
                        .systemId("testSP")
                        .sourceAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddr("1234567890")
                        .destAddrTon(1)
                        .destAddrNpi(1)
                        .destinationAddr("1234567890")
                        .esmClass(64) // 64 is UDHI
                        .validityPeriod(0)
                        .registeredDelivery(0)
                        .dataCoding(0) // 8 is unicode data coding
                        .shortMessage("This is a multipart test with data coding unicode and UDHI - part 1")
                        .isDlr(false)
                        .msgReferenceNumber("1")
                        .segmentSequence(1)
                        .totalSegment(2)
                        .build(),
                MessageEvent.builder()
                        .systemId("testSP")
                        .sourceAddrTon(1)
                        .sourceAddrNpi(1)
                        .sourceAddr("1234567890")
                        .destAddrTon(1)
                        .destAddrNpi(1)
                        .destinationAddr("1234567890")
                        .esmClass(64) // 64 is UDHI
                        .validityPeriod(0)
                        .registeredDelivery(0)
                        .dataCoding(0) // 8 is unicode data coding
                        .shortMessage("This is a multipart test with data coding unicode and UDHI - part 2")
                        .isDlr(false)
                        .msgReferenceNumber("1")
                        .segmentSequence(2)
                        .totalSegment(2)
                        .build()
        );
    }

    private MessageEvent getDeliverSM(String messageId) {
        return MessageEvent.builder()
                .id(messageId)
                .deliverSmId("52b3afdb-565f-456c-8dff-97233d3afa88")
                .deliverSmServerId(messageId)
                .systemId("systemId123")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50558499393")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50511112222")
                .esmClass(5)
                .validityPeriod(60)
                .registeredDelivery(0)
                .dataCoding(0)
                .smDefaultMsgId(0)
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(2)
                .destNetworkType("GW")
                .destProtocol("SMPP")
                .destNetworkId(1)
                .routingId(1)
                .isDlr(true)
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .optionalParameters(List.of(new UtilsRecords.OptionalParameter((short) 30, "52b3afdb-565f-456c-8dff-97233d3afa88")))
                .build();
    }

    private static List<UtilsRecords.OptionalParameter> getOptionalParameters(String sequencePart) {
        List<UtilsRecords.OptionalParameter> optionalParameters = new ArrayList<>();
        UtilsRecords.OptionalParameter referenceNumber = new UtilsRecords.OptionalParameter((short) 524, "1");
        UtilsRecords.OptionalParameter totalSegment = new UtilsRecords.OptionalParameter((short) 526, "2");
        UtilsRecords.OptionalParameter sequence = new UtilsRecords.OptionalParameter((short) 527, sequencePart);
        UtilsRecords.OptionalParameter languageIndicator = new UtilsRecords.OptionalParameter((short) 525, "1");

        optionalParameters.add(referenceNumber);
        optionalParameters.add(totalSegment);
        optionalParameters.add(sequence);
        optionalParameters.add(languageIndicator);

        return optionalParameters;
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

    private ServiceProvider getServiceProviderByBindTypes(String bindType) {
        return ServiceProvider.builder()
                .networkId(1)
                .systemId("successSmppSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType(bindType)
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .addressRange("4455")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(2)
                .smppServerId(1)
                .build();
    }

    private void setDifferentServiceProvider() {
        serviceProviderWithoutCredit = ServiceProvider.builder()
                .networkId(2)
                .systemId("hasNotCreditSP")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(false)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        stoppedServiceProvider = ServiceProvider.builder()
                .networkId(3)
                .systemId("stoppedSmppSP")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STOPPED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        invalidBindTypeServiceProvider = ServiceProvider.builder()
                .networkId(4)
                .systemId("invalidBindSP")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        invalidSmppServerIdServiceProvider = ServiceProvider.builder()
                .networkId(4)
                .systemId("invalidSmppServerId")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(4)
                .build();

        invalidPasswordServiceProvider = ServiceProvider.builder()
                .networkId(5)
                .systemId("invalidPassword")
                .password("invalidPassword")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        invalidInterfaceVersionServiceProvider = ServiceProvider.builder()
                .networkId(6)
                .systemId("invalidInterfaceVersionSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_33")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        ifAnyInterfaceVersionServiceProvider = ServiceProvider.builder()
                .networkId(10)
                .systemId("ifAnyInterfaceVersionSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_ANY")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        nullInterfaceVersionServiceProvider = ServiceProvider.builder()
                .networkId(11)
                .systemId("nullInterfaceVersionSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion(null)
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        invalidAddressRangeServiceProvider = ServiceProvider.builder()
                .networkId(7)
                .systemId("successSmppSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("addr")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        invalidSystemTypeServiceProvider = ServiceProvider.builder()
                .networkId(8)
                .systemId("successSmppSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .status("STARTED")
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSCEIVER")
                .systemType(null)
                .addressNpi(1)
                .addressTon(1)
                .addressRange("")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(1)
                .smppServerId(1)
                .build();

        okServiceProviderBindTransmitter = ServiceProvider.builder()
                .networkId(9)
                .systemId("transmitterSp")
                .password("1234")
                .protocol("SMPP")
                .binds(new ArrayList<>())
                .enabled(1)
                .status("STARTED")
                .messagePriority(GeneralSmscConstants.HIGH_PRIORITY)
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .bindType("TRANSMITTER")
                .systemType("SMPP")
                .addressNpi(1)
                .addressTon(1)
                .addressRange("4455")
                .interfaceVersion("IF_50")
                .currentBindsCount(0)
                .hasAvailableCredit(true)
                .maxBinds(2)
                .smppServerId(1)
                .build();
    }

    private static void toSleep(int seconds) {
        log.info("Sleeping for {} seconds", seconds);
        await().atMost(seconds, TimeUnit.SECONDS).until(() -> true);
    }

    private static int getRandomLocalPort() {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            return serverSocket.getLocalPort();
        } catch (Exception e) {
            log.error("Error while creating server socket", e);
            return 0;
        }
    }
}
