package com.paicbd.module.server;

import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.utils.ConcatenatedMessageDetails;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.Udh;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaUtils;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.MessageIDGeneratorImpl;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.utils.SmppUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.PDUStringException;
import org.jsmpp.SMPPConstant;
import org.jsmpp.bean.BroadcastSm;
import org.jsmpp.bean.CancelBroadcastSm;
import org.jsmpp.bean.CancelSm;
import org.jsmpp.bean.DataSm;
import org.jsmpp.bean.EnquireLink;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.QueryBroadcastSm;
import org.jsmpp.bean.QuerySm;
import org.jsmpp.bean.ReplaceSm;
import org.jsmpp.bean.SubmitMulti;
import org.jsmpp.bean.SubmitSm;
import org.jsmpp.extra.ProcessRequestException;
import org.jsmpp.session.BroadcastSmResult;
import org.jsmpp.session.DataSmResult;
import org.jsmpp.session.QueryBroadcastSmResult;
import org.jsmpp.session.QuerySmResult;
import org.jsmpp.session.SMPPServerSession;
import org.jsmpp.session.ServerMessageReceiverListener;
import org.jsmpp.session.Session;
import org.jsmpp.session.SubmitMultiResult;
import org.jsmpp.session.SubmitSmResult;
import org.jsmpp.util.MessageIDGenerator;
import org.jsmpp.util.MessageId;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

@Slf4j
@RequiredArgsConstructor
public class ServerMessageReceiverListenerImpl implements ServerMessageReceiverListener {
    private static final Set<Short> requiredTagsForConcatenatedSms = Set.of(
            OptionalParameter.Tag.SAR_MSG_REF_NUM.code(),
            OptionalParameter.Tag.SAR_TOTAL_SEGMENTS.code(),
            OptionalParameter.Tag.SAR_SEGMENT_SEQNUM.code()
    );

    private final MessageIDGenerator messageIDGenerator = new MessageIDGeneratorImpl();

    private final AtomicInteger requestCounter;
    private final SpSession spSession;
    private final GeneralSettingsCacheConfig generalSettingsCacheConfig;
    private final MultiPartsHandler multiPartsHandler;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RateLimiterUtilManager rateLimiterUtilManager;
    private final ProxyResponseHandler proxyResponseHandler;
    private final long proxyModeResponseTimeout;


    @Override
    public SubmitSmResult onAcceptSubmitSm(SubmitSm submitSm, SMPPServerSession smppServerSession) throws ProcessRequestException {

        String networkId = String.valueOf(spSession.getCurrentServiceProvider().getNetworkId());

        if (!rateLimiterUtilManager.tryConsume(networkId, spSession.getCurrentServiceProvider().getTps().get())) {
            log.error("Rate limit exceeded for Network ID: {}", networkId);
            throw new ProcessRequestException("Throttling error", SMPPConstant.STAT_ESME_RTHROTTLED);
        }

        if (Boolean.FALSE.equals(spSession.hasAvailableCredit())) {
            log.info("The credits has been exhausted for session: {}", smppServerSession.getSessionId());
            throw new ProcessRequestException("Throttling error", SMPPConstant.STAT_ESME_RTHROTTLED);
        }
        String remoteIp = smppServerSession.getInetAddress().getHostAddress();
        MessageId messageId = messageIDGenerator.newMessageId();

        if (spSession.getCurrentServiceProvider().isProxyMode()) {
            return handleProxySubmitSm(submitSm, messageId, remoteIp);
        }

        addInQ(submitSm, messageId, remoteIp, false);
        requestCounter.incrementAndGet();
        return new SubmitSmResult(messageId, new OptionalParameter[0]);
    }

    private SubmitSmResult handleProxySubmitSm(SubmitSm submitSm, MessageId messageId, String remoteIp)
            throws ProcessRequestException {
        String messageIdValue = messageId.getValue();

        proxyResponseHandler.register(messageIdValue);
        addInQ(submitSm, messageId, remoteIp, true);
        requestCounter.incrementAndGet();

        UtilsRecords.HttpProxyResponse proxyResponse = proxyResponseHandler.waitForResponse(messageIdValue, proxyModeResponseTimeout);

        if (Objects.isNull(proxyResponse)) {
            log.warn("Proxy mode: no downstream confirmation for submit_sm {} within {} ms",
                    messageIdValue, proxyModeResponseTimeout);
            throw new ProcessRequestException(
                    "Timeout waiting for downstream proxy confirmation", SMPPConstant.STAT_ESME_RSYSERR);
        }

        if (proxyResponse.error()) {
            log.warn("Proxy mode: downstream reported failure for submit_sm {}: {}",
                    messageIdValue, proxyResponse.errorMessage());
            throw new ProcessRequestException(
                    "Downstream proxy delivery failed: " + proxyResponse.errorMessage(),
                    resolveCommandStatus(proxyResponse.errorCode()));
        }

        log.debug("Proxy mode: downstream confirmed submit_sm {}", messageIdValue);
        return new SubmitSmResult(resolveResponseMessageId(messageId, proxyResponse), new OptionalParameter[0]);
    }

    private MessageId resolveResponseMessageId(MessageId internalMessageId, UtilsRecords.HttpProxyResponse proxyResponse) {
        String gatewayMessageId = proxyResponse.gatewayMessageId();
        if (Objects.isNull(gatewayMessageId) || gatewayMessageId.isBlank()) {
            return internalMessageId;
        }
        try {
            return new MessageId(gatewayMessageId);
        } catch (PDUStringException e) {
            log.warn("Proxy mode: invalid gateway message id '{}' for submit_sm {}, keeping internal id",
                    gatewayMessageId, internalMessageId.getValue());
            return internalMessageId;
        }
    }

    private int resolveCommandStatus(Integer errorCode) {
        return Objects.nonNull(errorCode) && errorCode != 0 ? errorCode : SMPPConstant.STAT_ESME_RSYSERR;
    }

    private void addInQ(SubmitSm submitSm, MessageId messageId, String remoteIp, boolean useProxy) {
        ServiceProvider currentServiceProvider = spSession.getCurrentServiceProvider();
        GeneralSettings smppGeneralSettings = generalSettingsCacheConfig.getCurrentGeneralSettings();
        int encodingType = SmppUtils.determineEncodingType(submitSm.getDataCoding(), smppGeneralSettings);
        MessageEvent submitSmEvent = createSubmitSmEvent(submitSm, messageId, currentServiceProvider, encodingType);
        submitSmEvent.setOriginNetworkType("SP");
        submitSmEvent.setOriginProtocol("SMPP");
        submitSmEvent.setUseProxy(useProxy);
        submitSmEvent.addHost(remoteIp);

        String kafkaTopic = KafkaUtils.getRoutingTopicPriority(currentServiceProvider.getMessagePriority());
        log.debug("Adding SubmitSm {} to {} topic.", submitSmEvent, kafkaTopic);
        ConcatenatedMessageDetails details = fetchConcatenatedMessageDetails(submitSm, submitSmEvent);
        submitSmEvent.setSmscMessagePriority(currentServiceProvider.getMessagePriority());
        if (Objects.nonNull(details)) {
            if (useProxy) {
                applyMultipartSegment(submitSmEvent, details, submitSm);
                kafkaTemplate.send(kafkaTopic, submitSmEvent.toString());
                return;
            }
            multiPartsHandler.messagePartsProcessor(submitSmEvent, details, submitSm, kafkaTopic);
            return;
        }
        kafkaTemplate.send(kafkaTopic, submitSmEvent.toString());
    }

    private void applyMultipartSegment(MessageEvent submitSmEvent, ConcatenatedMessageDetails details, SubmitSm submitSm) {
        submitSmEvent.setShortMessage(details.text());
        submitSmEvent.setMsgReferenceNumber(String.valueOf(details.msgReferenceNumber()));
        submitSmEvent.setTotalSegment(details.totalSegments());
        submitSmEvent.setSegmentSequence(details.segmentSequence());
        submitSmEvent.setMessageBytes(getPartBytes(submitSm));
        submitSmEvent.setUdhBytes(getUdhBytes(submitSm));
    }

    private byte[] getPartBytes(SubmitSm submitSm) {
        return submitSm.isUdhi()
                ? EncodingUtils.getCleanedBytes(submitSm.getShortMessage())
                : submitSm.getShortMessage();
    }

    private byte[] getUdhBytes(SubmitSm submitSm) {
        return submitSm.isUdhi()
                ? EncodingUtils.getUdhBytes(submitSm.getShortMessage())
                : new byte[0];
    }

    private ConcatenatedMessageDetails fetchConcatenatedMessageDetails(SubmitSm submitSm, MessageEvent messageEvent) {
        ConcatenatedMessageDetails concatenatedMessageDetails = null;
        if (submitSm.isUdhi() && !messageEvent.getShortMessage().isEmpty()) {
            concatenatedMessageDetails = validateConcatenationViaUdh(submitSm, messageEvent);
        }

        if (Objects.isNull(concatenatedMessageDetails) && Objects.nonNull(submitSm.getOptionalParameters())) {
            concatenatedMessageDetails = validateConcatenationViaTlv(submitSm, messageEvent);
        }

        return concatenatedMessageDetails;
    }

    private MessageEvent createSubmitSmEvent(
            SubmitSm submitSm, MessageId messageId, ServiceProvider currentServiceProvider, int encodingType) {
        MessageEvent event = getSubmitSmEvent(submitSm, encodingType);
        event.setSystemId(currentServiceProvider.getSystemId());
        event.setOriginNetworkId(currentServiceProvider.getNetworkId());
        event.setId(messageId.getValue());
        event.setMessageId(messageId.getValue());
        event.setParentId(messageId.getValue());

        if (submitSm.getOptionalParameters() != null) {
            SmppUtils.setTLV(event, submitSm.getOptionalParameters());
        }

        return event;
    }

    private MessageEvent getSubmitSmEvent(SubmitSm submitSm, int encodingType) {
        MessageEvent submitSmEvent = new MessageEvent();

        submitSmEvent.setCommandId(submitSm.getCommandId());
        submitSmEvent.setCommandLength(submitSm.getCommandLength());
        submitSmEvent.setCommandStatus(submitSm.getCommandStatus());
        submitSmEvent.setMessageLength(submitSm.getShortMessage().length);
        submitSmEvent.setServiceType(submitSm.getServiceType());
        submitSmEvent.setProtocolId(submitSm.getProtocolId());
        submitSmEvent.setPriorityFlag(submitSm.getPriorityFlag());
        submitSmEvent.setReplaceIfPresent(submitSm.getReplaceIfPresent());
        submitSmEvent.setScheduleDeliveryTime(submitSm.getScheduleDeliveryTime());
        submitSmEvent.setSmDefaultMsgId(submitSm.getSmDefaultMsgId());

        submitSmEvent.setRetry(false);
        submitSmEvent.setRetryDestNetworkId("");
        submitSmEvent.setCommandStatus(submitSm.getCommandStatus());
        submitSmEvent.setSequenceNumber(submitSm.getSequenceNumber());
        submitSmEvent.setSourceAddrTon((int) submitSm.getSourceAddrTon());
        submitSmEvent.setSourceAddrNpi((int) submitSm.getSourceAddrNpi());
        submitSmEvent.setSourceAddr(submitSm.getSourceAddr());
        submitSmEvent.setDestAddrTon((int) submitSm.getDestAddrTon());
        submitSmEvent.setDestAddrNpi((int) submitSm.getDestAddrNpi());
        submitSmEvent.setDestinationAddr(submitSm.getDestAddress());
        submitSmEvent.setEsmClass((int) submitSm.getEsmClass());
        submitSmEvent.setStringValidityPeriod(submitSm.getValidityPeriod());
        submitSmEvent.setRegisteredDelivery((int) submitSm.getRegisteredDelivery());
        submitSmEvent.setDataCoding((int) submitSm.getDataCoding());
        submitSmEvent.setSmDefaultMsgId(submitSm.getSmDefaultMsgId());

        byte[] shortMessageBytes = submitSm.getShortMessage().length > 0 ?
                submitSm.getShortMessage() :
                SmppUtils.getMessagePayloadValue(submitSm);

        String decodedShortMessage;
        byte[] udhBytes;
        byte[] cleanedBytes;
        if (submitSm.isUdhi()) {
            udhBytes = EncodingUtils.getUdhBytes(shortMessageBytes);
            cleanedBytes = EncodingUtils.getCleanedBytes(shortMessageBytes);
            decodedShortMessage = EncodingUtils.decodeMessage(cleanedBytes, encodingType);
            EncodingUtils.parseUdh(udhBytes, submitSmEvent);
        } else {
            decodedShortMessage = EncodingUtils.decodeMessage(shortMessageBytes, encodingType);
            cleanedBytes = EncodingUtils.encodeMessage(decodedShortMessage, encodingType);
        }

        submitSmEvent.setMessageBytes(cleanedBytes);
        submitSmEvent.setShortMessage(decodedShortMessage);
        return submitSmEvent;
    }

    private ConcatenatedMessageDetails validateConcatenationViaUdh(SubmitSm submitSm, MessageEvent messageEvent) {
        byte[] shortMessage = submitSm.getShortMessage().length > 0 ?
                submitSm.getShortMessage() :
                SmppUtils.getMessagePayloadValue(submitSm);
        submitSm.setShortMessage(shortMessage);
        if (shortMessage == null || shortMessage.length == 0) {
            return null;
        }

        EncodingUtils.parseUdh(shortMessage, messageEvent);
        Optional<Udh> maybeUdh = messageEvent.getUdhRaw().stream()
                .filter(udh -> "00".equals(udh.iei()) || "08".equals(udh.iei()))
                .findFirst();

        if (maybeUdh.isEmpty()) {
            return null;
        }

        Udh udh = maybeUdh.get();
        byte[] ieiContent = EncodingUtils.hexToBytes(udh.content());

        int referenceNumber;
        int totalSegments;
        int segmentSequence;

        switch (udh.iei()) {
            case "00" -> {
                if (ieiContent.length < 5) return null;
                referenceNumber = Byte.toUnsignedInt(ieiContent[2]);
                totalSegments = Byte.toUnsignedInt(ieiContent[3]);
                segmentSequence = Byte.toUnsignedInt(ieiContent[4]);
            }
            case "08" -> {
                if (ieiContent.length < 5) return null;
                referenceNumber = Byte.toUnsignedInt(ieiContent[2]) << 8 | Byte.toUnsignedInt(ieiContent[3]);
                totalSegments = Byte.toUnsignedInt(ieiContent[4]);
                segmentSequence = Byte.toUnsignedInt(ieiContent[5]);
            }
            default -> {
                return null;
            }
        }

        shortMessage = EncodingUtils.getCleanedBytes(submitSm.getShortMessage());
        log.debug("Concatenated message: referenceNumber={}, totalSegments={}, segmentSequence={}",
                referenceNumber, totalSegments, segmentSequence);
        int encodingType = SmppUtils.determineEncodingType(messageEvent.getDataCoding(),
                generalSettingsCacheConfig.getCurrentGeneralSettings());
        String decodedMessage = EncodingUtils.decodeMessage(shortMessage, encodingType);
        return new ConcatenatedMessageDetails(referenceNumber, totalSegments, segmentSequence, decodedMessage);
    }


    private ConcatenatedMessageDetails validateConcatenationViaTlv(SubmitSm submitSm, MessageEvent messageEvent) {
        if (submitSm.getOptionalParameters() == null || submitSm.getOptionalParameters().length < 3) {
            return null;
        }

        boolean containsAll = Arrays.stream(submitSm.getOptionalParameters())
                .map(optionalParameter -> optionalParameter.tag)
                .collect(Collectors.toSet())
                .containsAll(requiredTagsForConcatenatedSms);
        if (!containsAll) {
            return null;
        }

        int msgReferenceNumber = 0;
        int totalSegments = 0;
        int segmentSequence = 0;

        for (UtilsRecords.OptionalParameter optionalParameter : messageEvent.getOptionalParameters()) {
            if (optionalParameter.tag() == OptionalParameter.Tag.SAR_MSG_REF_NUM.code()) {
                msgReferenceNumber = Integer.parseInt(optionalParameter.value());
            }
            if (optionalParameter.tag() == OptionalParameter.Tag.SAR_TOTAL_SEGMENTS.code()) {
                totalSegments = Integer.parseInt(optionalParameter.value());
            }
            if (optionalParameter.tag() == OptionalParameter.Tag.SAR_SEGMENT_SEQNUM.code()) {
                segmentSequence = Integer.parseInt(optionalParameter.value());
            }
        }

        byte[] shortMessage = submitSm.getShortMessage().length > 0 ?
                submitSm.getShortMessage() :
                SmppUtils.getMessagePayloadValue(submitSm);
        submitSm.setShortMessage(shortMessage);
        int encodingType = SmppUtils.determineEncodingType(messageEvent.getDataCoding(),
                generalSettingsCacheConfig.getCurrentGeneralSettings());

        return new ConcatenatedMessageDetails(
                msgReferenceNumber, totalSegments, segmentSequence,
                EncodingUtils.decodeMessage(shortMessage, encodingType));
    }

    @Generated
    @Override
    public SubmitMultiResult onAcceptSubmitMulti(SubmitMulti submitMulti, SMPPServerSession smppServerSession) {
        return null;
    }

    @Generated
    @Override
    public QuerySmResult onAcceptQuerySm(QuerySm querySm, SMPPServerSession smppServerSession) {
        return null;
    }

    @Generated
    @Override
    public void onAcceptReplaceSm(ReplaceSm replaceSm, SMPPServerSession smppServerSession) {
        log.info("ReplaceSm received: {}", replaceSm);
    }

    @Generated
    @Override
    public void onAcceptCancelSm(CancelSm cancelSm, SMPPServerSession smppServerSession) {
        log.info("CancelSm received: {}", cancelSm);
    }

    @Generated
    @Override
    public BroadcastSmResult onAcceptBroadcastSm(BroadcastSm broadcastSm, SMPPServerSession smppServerSession) {
        return null;
    }

    @Generated
    @Override
    public void onAcceptCancelBroadcastSm(CancelBroadcastSm cancelBroadcastSm, SMPPServerSession smppServerSession) {
        log.info("CancelBroadcastSm received: {}", cancelBroadcastSm);
    }

    @Generated
    @Override
    public QueryBroadcastSmResult onAcceptQueryBroadcastSm(QueryBroadcastSm queryBroadcastSm, SMPPServerSession smppServerSession) {
        return null;
    }

    @Generated
    @Override
    public DataSmResult onAcceptDataSm(DataSm dataSm, Session session) {
        return null;
    }

    @Generated
    @Override
    public void onAcceptEnquireLink(EnquireLink enquireLink, Session source) {
        ServerMessageReceiverListener.super.onAcceptEnquireLink(enquireLink, source);
    }
}
