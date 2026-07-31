package com.paicbd.module.components;

import com.paicbd.module.config.PendingDlrCache;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.UtilsEnum;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.MessageType;
import org.jsmpp.bean.NumberingPlanIndicator;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.bean.TypeOfNumber;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.SMPPServerSession;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DeliverSmQueueConsumerTest {

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;
    @Mock
    ConcurrentMap<Integer, SpSession> spSessionMap;
    @Mock
    GeneralSettingsCacheConfig generalSettingsCacheConfig;
    @Mock
    SpSession spSessionMock;
    @Mock
    SMPPServerSession serverSession;
    @Mock
    PendingDlrCache pendingDlrCacheMock;

    @Test
    @DisplayName("complete deliver_sm flow sends the message to the bound service provider session")
    void startSchedulerWhenExecuteCompleteFlowThenDoItSuccessfully() throws ResponseTimeoutException, PDUException, IOException, InvalidResponseException, NegativeResponseException {
        int destNetworkId = 1;
        MessageEvent deliverSmEvent = MessageEvent.builder()
                .id("1719421854353-11028072268459")
                .deliverSmId("52b3afdb-565f-456c-8dff-97233d3afa88")
                .deliverSmServerId("1719421854353-11028072268459")
                .systemId("systemId123")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50510201020")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
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
                .destNetworkId(destNetworkId)
                .routingId(1)
                .isDlr(true)
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .optionalParameters(List.of(new UtilsRecords.OptionalParameter((short) 30, "52b3afdb-565f-456c-8dff-97233d3afa88")))
                .build();

        GeneralSettings generalSettingsMock = GeneralSettings.builder()
                .id(1)
                .validityPeriod(60)
                .maxValidityPeriod(240)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(EncodingUtils.ISO88591)
                .encodingGsm7(EncodingUtils.GSM7)
                .encodingUcs2(EncodingUtils.UCS2)
                .build();

        when(spSessionMock.getNextRoundRobinSession()).thenReturn(serverSession);
        when(spSessionMap.get(1)).thenReturn(spSessionMock);
        when(generalSettingsCacheConfig.getCurrentGeneralSettings()).thenReturn(generalSettingsMock);
        ServiceProvider mockSp = ServiceProvider.builder().dlrTlvEnabled(true).build();
        when(spSessionMock.getCurrentServiceProvider()).thenReturn(mockSp);

        DeliverSmQueueConsumer consumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMap, generalSettingsCacheConfig);
        consumer.handleDeliverySms(List.of(deliverSmEvent.toString()));
        toSleep();

        verify(spSessionMap).get(1);
        verify(generalSettingsCacheConfig).getCurrentGeneralSettings();

        ArgumentCaptor<String> sourceAddrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeOfNumber> sourceAddrTonCaptor = ArgumentCaptor.forClass(TypeOfNumber.class);
        ArgumentCaptor<NumberingPlanIndicator> sourceAddrNpiCaptor = ArgumentCaptor.forClass(NumberingPlanIndicator.class);
        ArgumentCaptor<String> destAddrCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeOfNumber> destAddrTonCaptor = ArgumentCaptor.forClass(TypeOfNumber.class);
        ArgumentCaptor<NumberingPlanIndicator> destAddrNpiCaptor = ArgumentCaptor.forClass(NumberingPlanIndicator.class);
        ArgumentCaptor<DataCoding> dataCodingCaptor = ArgumentCaptor.forClass(DataCoding.class);
        ArgumentCaptor<byte[]> encodedShortMessageCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<OptionalParameter> optionalParameterArgumentCaptor = ArgumentCaptor.forClass(OptionalParameter.class);
        ArgumentCaptor<ESMClass> esmClassArgumentCaptor = ArgumentCaptor.forClass(ESMClass.class);
        ArgumentCaptor<String> serviceTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Byte> protocolIdCaptor = ArgumentCaptor.forClass(Byte.class);
        ArgumentCaptor<Byte> priorityFlagCaptor = ArgumentCaptor.forClass(Byte.class);
        ArgumentCaptor<RegisteredDelivery> registeredDeliveryCaptor = ArgumentCaptor.forClass(RegisteredDelivery.class);
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
                optionalParameterArgumentCaptor.capture()
        );

        assertEquals(deliverSmEvent.getSourceAddr(), sourceAddrCaptor.getValue());
        assertEquals(UtilsEnum.getTypeOfNumber(deliverSmEvent.getSourceAddrTon()), sourceAddrTonCaptor.getValue());
        assertEquals(deliverSmEvent.getDestinationAddr(), destAddrCaptor.getValue());
        assertEquals(UtilsEnum.getTypeOfNumber(deliverSmEvent.getDestAddrTon()), destAddrTonCaptor.getValue());
        assertEquals(UtilsEnum.getNumberingPlanIndicator(deliverSmEvent.getDestAddrNpi()), destAddrNpiCaptor.getValue());
        assertEquals(UtilsEnum.getNumberingPlanIndicator(deliverSmEvent.getSourceAddrNpi()), sourceAddrNpiCaptor.getValue());
        assertEquals(EncodingUtils.getDataCoding(deliverSmEvent.getDataCoding()), dataCodingCaptor.getValue());
        assertArrayEquals(deliverSmEvent.getDelReceipt().getBytes(), encodedShortMessageCaptor.getValue());
            assertEquals(30, optionalParameterArgumentCaptor.getValue().tag);
        assertTrue(MessageType.SMSC_DEL_RECEIPT.containedIn(esmClassArgumentCaptor.getValue()));
    }

    @Test
    @DisplayName("when no active server session the deliver_sm is placed in the SpSession's pending DLR cache")
    void startSchedulerWhenNoActiveServerSessionThenDeliversSmIsQueuedInPendingCache() {
        int destNetworkId = 1;
        MessageEvent deliverSmEvent = MessageEvent.builder()
                .id("1719421854353-11028072268459")
                .messageId("1719421854353-11028072268459")
                .deliverSmId("52b3afdb-565f-456c-8dff-97233d3afa88")
                .deliverSmServerId("1719421854353-11028072268459")
                .systemId("systemId123")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50510201020")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .esmClass(5)
                .validityPeriod(60)
                .registeredDelivery(0)
                .dataCoding(0)
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(2)
                .destNetworkType("GW")
                .destProtocol("SMPP")
                .destNetworkId(destNetworkId)
                .routingId(1)
                .isDlr(true)
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .build();

        when(spSessionMock.getNextRoundRobinSession()).thenReturn(null);
        when(spSessionMap.get(destNetworkId)).thenReturn(spSessionMock);
        when(spSessionMock.getPendingDlrCache()).thenReturn(pendingDlrCacheMock);

        DeliverSmQueueConsumer consumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMap, generalSettingsCacheConfig);
        consumer.handleDeliverySms(List.of(deliverSmEvent.toString()));
        toSleep();

        ArgumentCaptor<MessageEvent> eventCaptor = ArgumentCaptor.forClass(MessageEvent.class);
        verify(pendingDlrCacheMock).put(eventCaptor.capture());
        assertEquals(deliverSmEvent.getMessageId(), eventCaptor.getValue().getMessageId());

        verify(spSessionMap).get(destNetworkId);
        verify(generalSettingsCacheConfig, never()).getCurrentGeneralSettings();
    }

    @Test
    @DisplayName("when no SpSession is registered for the networkId a failure CDR is published immediately")
    void startSchedulerWhenNoSpSessionRegisteredThenPublishesFailureCdr() {
        int destNetworkId = 1;
        MessageEvent deliverSmEvent = MessageEvent.builder()
                .id("1719421854353-11028072268459")
                .messageId("52b3afdb-565f-456c-8dff-97233d3afa88")
                .deliverSmId("52b3afdb-565f-456c-8dff-97233d3afa88")
                .deliverSmServerId("1719421854353-11028072268459")
                .systemId("systemId123")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50510201020")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .esmClass(5)
                .validityPeriod(60)
                .registeredDelivery(0)
                .dataCoding(0)
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(2)
                .destNetworkType("GW")
                .destProtocol("SMPP")
                .destNetworkId(destNetworkId)
                .routingId(1)
                .isDlr(true)
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .build();

        when(spSessionMap.get(destNetworkId)).thenReturn(null);

        DeliverSmQueueConsumer consumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMap, generalSettingsCacheConfig);
        consumer.handleDeliverySms(List.of(deliverSmEvent.toString()));
        toSleep();

        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), kafkaCaptor.capture());
        UtilsRecords.Cdr cdr = Converter.stringToObject(kafkaCaptor.getValue(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals("FAILED", cdr.status());
        assertEquals(String.valueOf(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION), cdr.statusCode());
        verifyNoMoreInteractions(spSessionMock);
        verifyNoMoreInteractions(generalSettingsCacheConfig);
        verifyNoMoreInteractions(serverSession);
    }

    @Test
    @DisplayName("when the Kafka message list is empty nothing is processed")
    void startSchedulerWhenRedisQueueIsEmptyThenDoNothing() {
        DeliverSmQueueConsumer consumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMap, generalSettingsCacheConfig);
        consumer.handleDeliverySms(List.of());
        toSleep();

        verifyNoMoreInteractions(spSessionMap);
        verifyNoMoreInteractions(spSessionMock);
        verifyNoMoreInteractions(generalSettingsCacheConfig);
        verifyNoMoreInteractions(serverSession);
    }

    @Test
    @DisplayName("when a message cannot be deserialized from JSON it is silently skipped")
    void startSchedulerWhenProcessDeliverSmAndDeliverSmEventInvalidFormatThenNotTryToSend() {
        DeliverSmQueueConsumer consumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMap, generalSettingsCacheConfig);
        consumer.handleDeliverySms(List.of("incorrect:json}"));
        toSleep();

        verifyNoMoreInteractions(spSessionMap);
        verifyNoMoreInteractions(spSessionMock);
        verifyNoMoreInteractions(generalSettingsCacheConfig);
        verifyNoMoreInteractions(serverSession);
    }

    @Test
    @DisplayName("when getNextRoundRobinSession throws an unexpected exception the message is not sent")
    void startSchedulerWhenProcessDeliverSmAndGettingExceptionThenNotTryToSend() {
        int destNetworkId = 1;
        MessageEvent deliverSmEvent = MessageEvent.builder()
                .id("1719421854353-11028072268459")
                .deliverSmId("52b3afdb-565f-456c-8dff-97233d3afa88")
                .deliverSmServerId("1719421854353-11028072268459")
                .systemId("systemId123")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50510201020")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .esmClass(5)
                .validityPeriod(60)
                .registeredDelivery(0)
                .dataCoding(0)
                .shortMessage("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(2)
                .destNetworkType("GW")
                .destProtocol("SMPP")
                .destNetworkId(destNetworkId)
                .routingId(1)
                .isDlr(true)
                .delReceipt("id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message")
                .build();

        when(spSessionMap.get(destNetworkId)).thenReturn(spSessionMock);
        when(spSessionMock.getNextRoundRobinSession()).thenThrow(new RuntimeException("exception to get getNextRoundRobinSession"));

        DeliverSmQueueConsumer consumer = new DeliverSmQueueConsumer(kafkaTemplate, spSessionMap, generalSettingsCacheConfig);
        consumer.handleDeliverySms(List.of(deliverSmEvent.toString()));
        toSleep();

        verifyNoMoreInteractions(generalSettingsCacheConfig);
        verifyNoMoreInteractions(serverSession);
    }

    private static void toSleep() {
        await().atMost(ONE_SECOND).until(() -> true);
    }
}
