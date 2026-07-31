package com.paicbd.module.server;

import com.paicbd.module.utils.ConcatenatedMessageDetails;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.jsmpp.bean.SubmitSm;
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MultiPartsHandlerTest {
    @Mock
    ScyllaManager scyllaManager;
    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    private MultiPartsHandler multiPartsHandler;

    private static final int MESSAGE_PARTS_TTL = 30;
    private static final String SYSTEM_ID = "smppSp";
    private static final int REF_NUMBER = 1;
    private static final String KEY = SYSTEM_ID + "|" + REF_NUMBER;

    @BeforeEach
    void setUp() {
        multiPartsHandler = new MultiPartsHandler(scyllaManager, kafkaTemplate, MESSAGE_PARTS_TTL);
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
    @DisplayName("Two parts received on the same handler instance complete the multipart message")
    void processPartWhenMessageHasTwoPartsThenDoItSuccessfully(String priority) {
        String firstPart = "Hello I hope you are doing well I wanted to remind you that our meeting is tomorrow";
        String secondPart = "If you have any questions or need to change the time let me know";

        SubmitSm submitSm = mock(SubmitSm.class);
        when(submitSm.getShortMessage()).thenReturn(firstPart.getBytes());
        // First call: no snapshot yet — handler creates a new accumulator.
        // The accumulator stays in the in-memory map, so no Scylla GET happens on the second call.

        multiPartsHandler.messagePartsProcessor(
                buildEvent(firstPart, 1),
                new ConcatenatedMessageDetails(REF_NUMBER, 2, 1, firstPart),
                submitSm,
                KafkaUtils.getRoutingTopicPriority(priority));

        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> snapshotCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Integer> ttlCaptor = ArgumentCaptor.forClass(Integer.class);
        verify(scyllaManager).insertIntoSmppMessageParts(keyCaptor.capture(), snapshotCaptor.capture(), ttlCaptor.capture());
        assertEquals(KEY, keyCaptor.getValue());
        assertEquals(MESSAGE_PARTS_TTL, ttlCaptor.getValue());
        // Snapshot now stores the full MessageEvent (JSON), not just the part text.
        MessageEvent persistedEvent = Converter.stringToObject(snapshotCaptor.getValue(), MessageEvent.class);
        assertNotNull(persistedEvent);
        assertEquals(1, persistedEvent.getMessageParts().size());

        when(submitSm.getShortMessage()).thenReturn(secondPart.getBytes());
        multiPartsHandler.messagePartsProcessor(
                buildEvent(secondPart, 2),
                new ConcatenatedMessageDetails(REF_NUMBER, 2, 2, secondPart),
                submitSm,
                KafkaUtils.getRoutingTopicPriority(priority));

        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> finalMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(topicCaptor.capture(), finalMessageCaptor.capture());
        verify(scyllaManager).deleteSmppMessagePartsByKey(KEY);

        switch (priority) {
            case GeneralSmscConstants.HIGH_PRIORITY ->
                    assertEquals(KafkaTopicsConstants.PRE_MESSAGE_HIGH_TOPIC, topicCaptor.getValue());
            case GeneralSmscConstants.MEDIUM_PRIORITY ->
                    assertEquals(KafkaTopicsConstants.PRE_MESSAGE_MEDIUM_TOPIC, topicCaptor.getValue());
            case GeneralSmscConstants.LOW_PRIORITY ->
                    assertEquals(KafkaTopicsConstants.PRE_MESSAGE_LOW_TOPIC, topicCaptor.getValue());
            default -> throw new IllegalStateException("Unexpected value: " + priority);
        }

        MessageEvent finalEvent = Converter.stringToObject(finalMessageCaptor.getValue(), MessageEvent.class);
        assertNotNull(finalEvent);
        assertEquals(2, finalEvent.getMessageParts().size());
        assertEquals(2, finalEvent.getMessageParts().getFirst().getTotalSegment());
    }

    @Test
    @DisplayName("Five parts on the SAME shared handler (simulating 5 binds) complete the multipart message")
    void fivePartsSharedHandlerCompleteMessage() {
        SubmitSm submitSm = mock(SubmitSm.class);
        String topic = KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.MEDIUM_PRIORITY);

        for (int seq = 1; seq <= 5; seq++) {
            String partText = "part-" + seq;
            when(submitSm.getShortMessage()).thenReturn(partText.getBytes());
            multiPartsHandler.messagePartsProcessor(
                    buildEvent(partText, seq),
                    new ConcatenatedMessageDetails(REF_NUMBER, 5, seq, partText),
                    submitSm,
                    topic);
        }

        // Insert called for parts 1..4 (not for the final part, since it triggers completion + delete instead).
        verify(scyllaManager, times(4)).insertIntoSmppMessageParts(eq(KEY), anyString(), eq(MESSAGE_PARTS_TTL));
        verify(scyllaManager).deleteSmppMessagePartsByKey(KEY);

        ArgumentCaptor<String> finalMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(topic), finalMessageCaptor.capture());

        MessageEvent finalEvent = Converter.stringToObject(finalMessageCaptor.getValue(), MessageEvent.class);
        assertNotNull(finalEvent);
        assertEquals(5, finalEvent.getMessageParts().size());
        // Parts are ordered by segmentSequence in the final event
        for (int i = 0; i < 5; i++) {
            assertEquals(i + 1, finalEvent.getMessageParts().get(i).getSegmentSequence());
        }
    }

    @Test
    @DisplayName("Rehydrate from Scylla when in-memory accumulator is missing")
    void rehydrateFromScyllaWhenAppenderNotInMemory() {
        // Build the snapshot directly (as if a previous handler/process had persisted it).
        SubmitSm submitSm = mock(SubmitSm.class);
        when(submitSm.getShortMessage()).thenReturn("hello-1".getBytes());
        when(submitSm.isUdhi()).thenReturn(false);

        MessageEvent firstPartEvent = buildEvent("hello-1", 1);
        ConcatenationAppender seedAppender = new ConcatenationAppender(2, firstPartEvent);
        seedAppender.initialize(firstPartEvent,
                new ConcatenatedMessageDetails(REF_NUMBER, 2, 1, "hello-1"),
                submitSm);
        String storedSnapshot = seedAppender.snapshot();

        // A fresh handler (in-memory map empty) receives part 2 — it must rehydrate from Scylla.
        when(scyllaManager.getSmppMessagePartsByKey(KEY)).thenReturn(storedSnapshot);
        when(submitSm.getShortMessage()).thenReturn("hello-2".getBytes());

        multiPartsHandler.messagePartsProcessor(
                buildEvent("hello-2", 2),
                new ConcatenatedMessageDetails(REF_NUMBER, 2, 2, "hello-2"),
                submitSm,
                KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.LOW_PRIORITY));

        verify(scyllaManager).getSmppMessagePartsByKey(KEY);
        verify(scyllaManager).deleteSmppMessagePartsByKey(KEY);

        ArgumentCaptor<String> finalMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.LOW_PRIORITY)),
                finalMessageCaptor.capture());

        MessageEvent finalEvent = Converter.stringToObject(finalMessageCaptor.getValue(), MessageEvent.class);
        assertNotNull(finalEvent);
        assertEquals(2, finalEvent.getMessageParts().size());
        assertEquals(1, finalEvent.getMessageParts().get(0).getSegmentSequence());
        assertEquals(2, finalEvent.getMessageParts().get(1).getSegmentSequence());
    }

    @Test
    @DisplayName("Out-of-order parts are sorted by segmentSequence in the final event")
    void outOfOrderPartsAreSortedInFinalEvent() {
        SubmitSm submitSm = mock(SubmitSm.class);
        String topic = KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.MEDIUM_PRIORITY);

        // Parts arrive in order: 3, 1, 2
        for (int seq : new int[]{3, 1, 2}) {
            String partText = "part-" + seq;
            when(submitSm.getShortMessage()).thenReturn(partText.getBytes());
            multiPartsHandler.messagePartsProcessor(
                    buildEvent(partText, seq),
                    new ConcatenatedMessageDetails(REF_NUMBER, 3, seq, partText),
                    submitSm,
                    topic);
        }

        ArgumentCaptor<String> finalMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq(topic), finalMessageCaptor.capture());

        MessageEvent finalEvent = Converter.stringToObject(finalMessageCaptor.getValue(), MessageEvent.class);
        assertNotNull(finalEvent);
        assertEquals(Arrays.asList(1, 2, 3),
                finalEvent.getMessageParts().stream().map(p -> p.getSegmentSequence()).toList());
    }

    @Test
    @DisplayName("Duplicate segmentSequence is ignored and does not prematurely complete the message")
    void duplicateSegmentIsIgnored() {
        SubmitSm submitSm = mock(SubmitSm.class);
        String topic = KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.LOW_PRIORITY);

        // Three parts expected; send seq=1, then seq=1 again (duplicate), then seq=2, then seq=3.
        for (int seq : new int[]{1, 1, 2, 3}) {
            String partText = "part-" + seq;
            when(submitSm.getShortMessage()).thenReturn(partText.getBytes());
            multiPartsHandler.messagePartsProcessor(
                    buildEvent(partText, seq),
                    new ConcatenatedMessageDetails(REF_NUMBER, 3, seq, partText),
                    submitSm,
                    topic);
        }

        // The duplicate should not have triggered completion early. Final Kafka send happens exactly once.
        ArgumentCaptor<String> finalMessageCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(1)).send(eq(topic), finalMessageCaptor.capture());

        MessageEvent finalEvent = Converter.stringToObject(finalMessageCaptor.getValue(), MessageEvent.class);
        assertNotNull(finalEvent);
        assertEquals(3, finalEvent.getMessageParts().size());
    }

    @Test
    @DisplayName("Unparseable Scylla snapshot is discarded and the part is treated as new")
    void unparseableSnapshotIsDiscarded() {
        when(scyllaManager.getSmppMessagePartsByKey(KEY)).thenReturn("not-a-json-snapshot");

        SubmitSm submitSm = mock(SubmitSm.class);
        when(submitSm.getShortMessage()).thenReturn("hello".getBytes());

        multiPartsHandler.messagePartsProcessor(
                buildEvent("hello", 1),
                new ConcatenatedMessageDetails(REF_NUMBER, 2, 1, "hello"),
                submitSm,
                KafkaUtils.getRoutingTopicPriority(GeneralSmscConstants.LOW_PRIORITY));

        // The legacy/garbled entry must be deleted, and the part stored as a brand-new accumulator.
        verify(scyllaManager).deleteSmppMessagePartsByKey(KEY);
        verify(scyllaManager).insertIntoSmppMessageParts(eq(KEY), anyString(), eq(MESSAGE_PARTS_TTL));
        verify(kafkaTemplate, never()).send(anyString(), anyString());
    }

    private MessageEvent buildEvent(String shortMessage, int segmentSequence) {
        List<UtilsRecords.OptionalParameter> optionalParameters = new ArrayList<>();
        optionalParameters.add(new UtilsRecords.OptionalParameter((short) 524, String.valueOf(REF_NUMBER)));
        optionalParameters.add(new UtilsRecords.OptionalParameter((short) 526, "2"));
        optionalParameters.add(new UtilsRecords.OptionalParameter((short) 527, String.valueOf(segmentSequence)));

        return MessageEvent.builder()
                .id("evt-" + segmentSequence)
                .messageId("evt-" + segmentSequence)
                .systemId(SYSTEM_ID)
                .commandStatus(0)
                .sequenceNumber(segmentSequence)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .sourceAddr("50510201020")
                .destAddrTon(1)
                .destAddrNpi(1)
                .destinationAddr("50582368999")
                .esmClass(64)
                .validityPeriod(60)
                .registeredDelivery(1)
                .dataCoding(0)
                .smDefaultMsgId(0)
                .shortMessage(shortMessage)
                .originNetworkType("SP")
                .originProtocol("SMPP")
                .originNetworkId(2)
                .destNetworkType("GW")
                .destProtocol("SMPP")
                .destNetworkId(1)
                .routingId(1)
                .isRetry(false)
                .isLastRetry(false)
                .isNetworkNotifyError(false)
                .dueDelay(0)
                .checkSriResponse(false)
                .isDlr(false)
                .optionalParameters(optionalParameters)
                .build();
    }
}
