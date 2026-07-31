package com.paicbd.module.server;

import com.paicbd.module.utils.ConcatenatedMessageDetails;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.MessagePart;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.MessageIDGeneratorImpl;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.SubmitSm;
import org.jsmpp.util.MessageIDGenerator;
import org.jsmpp.util.MessageId;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

@Slf4j
public class ConcatenationAppender {
    private final MessageIDGenerator messageIDGenerator = new MessageIDGeneratorImpl();

    @Getter
    private final int totalParts;
    @Getter
    private final MessageEvent incomingEvent;

    public ConcatenationAppender(int totalParts, MessageEvent incomingEvent) {
        this.totalParts = totalParts;
        this.incomingEvent = incomingEvent;
        if (this.incomingEvent.getMessageParts() == null) {
            this.incomingEvent.setMessageParts(new ArrayList<>());
        }
        log.debug("Initialized ConcatenationAppender with systemId: {} and total parts: {}",
                incomingEvent.getSystemId(), totalParts);
    }

    public void initialize(
            MessageEvent firstEvent,
            ConcatenatedMessageDetails details,
            SubmitSm submitSm) {
        MessageId newParentId = messageIDGenerator.newMessageId();
        String messageId = newParentId.toString();

        incomingEvent.setId(messageId);
        incomingEvent.setParentId(messageId);

        addPart(buildPart(firstEvent, details, submitSm));
    }

    public void addMessagePart(SubmitSm submitSm, ConcatenatedMessageDetails details, MessageEvent event) {
        addPart(buildPart(event, details, submitSm));
    }

    private void addPart(MessagePart segment) {
        boolean duplicate = incomingEvent.getMessageParts().stream()
                .anyMatch(p -> Objects.equals(p.getSegmentSequence(), segment.getSegmentSequence()));
        if (duplicate) {
            log.warn("Ignoring duplicate multipart segment: ref={} seq={} (already accumulated)",
                    segment.getMsgReferenceNumber(), segment.getSegmentSequence());
            return;
        }
        incomingEvent.getMessageParts().add(segment);
    }

    public boolean isComplete() {
        return incomingEvent.getMessageParts().size() == totalParts;
    }

    public MessageEvent finalizeEvent() {
        incomingEvent.getMessageParts().sort(Comparator.comparing(MessagePart::getSegmentSequence));
        return incomingEvent;
    }

    public String snapshot() {
        return Converter.valueAsString(incomingEvent);
    }

    public static ConcatenationAppender restore(String snapshot) {
        MessageEvent event = Converter.stringToObject(snapshot, MessageEvent.class);
        Objects.requireNonNull(event, "Failed to deserialize multipart snapshot");
        List<MessagePart> parts = event.getMessageParts();
        if (parts == null || parts.isEmpty()) {
            throw new IllegalStateException("Multipart snapshot has no parts");
        }
        return new ConcatenationAppender(parts.getFirst().getTotalSegment(), event);
    }

    private MessagePart buildPart(MessageEvent event, ConcatenatedMessageDetails details, SubmitSm submitSm) {
        return MessagePart.builder()
                .messageId(event.getMessageId())
                .shortMessage(details.text())
                .msgReferenceNumber(String.valueOf(details.msgReferenceNumber()))
                .totalSegment(details.totalSegments())
                .segmentSequence(details.segmentSequence())
                .optionalParameters(event.getOptionalParameters())
                .partBytes(getPartBytes(submitSm))
                .udhBytes(getUdhBytes(submitSm))
                .udhRaw(event.getUdhRaw())
                .build();
    }

    private byte[] getPartBytes(SubmitSm submitSm) {
        return submitSm.isUdhi() ?
                EncodingUtils.getCleanedBytes(submitSm.getShortMessage()) :
                submitSm.getShortMessage();
    }

    private byte[] getUdhBytes(SubmitSm submitSm) {
        return submitSm.isUdhi() ?
                EncodingUtils.getUdhBytes(submitSm.getShortMessage()) :
                new byte[0];
    }
}
