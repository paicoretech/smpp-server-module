package com.paicbd.module.utils;

import com.paicbd.smsc.cdr.CdrProcessor;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.kafka.KafkaTopicsConstants;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.RequestDelivery;
import com.paicbd.smsc.utils.SmppUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.GSMSpecificFeature;
import org.jsmpp.bean.MessageMode;
import org.jsmpp.bean.MessageType;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.extra.NegativeResponseException;
import org.jsmpp.extra.ResponseTimeoutException;
import org.jsmpp.session.SMPPServerSession;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Slf4j
public class StaticMethods {
    @Generated
    private StaticMethods() {
        throw new IllegalStateException("Utility Class");
    }

    private static final Set<Integer> validDataCodings = Set.of(EncodingUtils.GSM7, EncodingUtils.ISO88591, 8);
    private static final CdrProcessor cdrProcessor = new CdrProcessor();

    public static void sendDeliverSm(
            SMPPServerSession serverSession, MessageEvent deliverSmEvent, boolean dlrTLVsEnabled,
            GeneralSettings smppGeneralSettings, KafkaTemplate<String, String> kafkaTemplate) {
        try {
            List<UtilsRecords.OptionalParameter> optionalParameters = new ArrayList<>();
            if (deliverSmEvent.getOptionalParameters() != null) {
                optionalParameters = deliverSmEvent.getOptionalParameters();
                if (dlrTLVsEnabled) {
                    if (GeneralSmscConstants.SMPP_PROTOCOL.equals(deliverSmEvent.getOriginProtocol())) {
                        replaceOptionalParameter(optionalParameters, deliverSmEvent);
                        deliverSmEvent.setOptionalParameters(optionalParameters);
                    }
                } else {
                    removeOptionalParameter(optionalParameters);
                    deliverSmEvent.setOptionalParameters(optionalParameters);
                }
            }

            int dataCodingDlr = Objects.isNull(deliverSmEvent.getDataCoding()) ? 0 : deliverSmEvent.getDataCoding();
            int encodingType = SmppUtils.determineEncodingType(dataCodingDlr, smppGeneralSettings);
            DataCoding dataCoding = EncodingUtils.getDataCoding(dataCodingDlr);

            byte[] encodedShortMessage;
            if (deliverSmEvent.getMessageBytes() != null && deliverSmEvent.getMessageBytes().length > 0) {
                encodedShortMessage = deliverSmEvent.getMessageBytes();
            } else {
                encodedShortMessage = EncodingUtils.encodeMessage(
                        deliverSmEvent.getShortMessage() != null ? deliverSmEvent.getShortMessage() : "",
                        encodingType
                );
            }

            byte[] shortMessageToDeliver =
                    deliverSmEvent.getUdhLength() > 0 ?
                            EncodingUtils.prepend(deliverSmEvent.getUdhBytes(), encodedShortMessage) :
                            encodedShortMessage;

            serverSession.deliverShortMessage(
                    deliverSmEvent.getServiceType(),
                    UtilsEnum.getTypeOfNumber(deliverSmEvent.getSourceAddrTon()),
                    UtilsEnum.getNumberingPlanIndicator(deliverSmEvent.getSourceAddrNpi()),
                    deliverSmEvent.getSourceAddr(),
                    UtilsEnum.getTypeOfNumber(deliverSmEvent.getDestAddrTon()),
                    UtilsEnum.getNumberingPlanIndicator(deliverSmEvent.getDestAddrNpi()),
                    deliverSmEvent.getDestinationAddr(),
                    getEsmClass(deliverSmEvent.getEsmClass(), deliverSmEvent.getUdhLength() > 0),
                    deliverSmEvent.getProtocolId(),
                    deliverSmEvent.getPriorityFlag(),
                    new RegisteredDelivery(RequestDelivery.NON_REQUEST_DLR.getValue()),
                    dataCoding,
                    shortMessageToDeliver,
                    optionalParameters.isEmpty() ? null : SmppUtils.getTLV(deliverSmEvent));

            cdrProcessor.publishCdr(deliverSmEvent, UtilsEnum.Module.SMPP_SERVER,
                    UtilsEnum.MessageType.DELIVER, UtilsEnum.CdrStatus.SUCCESS, kafkaTemplate);
            sendProxyResponse(kafkaTemplate, deliverSmEvent, UtilsEnum.CdrStatus.SUCCESS);
        } catch (Exception e) {
            int errorCode = handleException(e, deliverSmEvent);
            deliverSmEvent.setErrorCode(errorCode);

            cdrProcessor.publishCdr(deliverSmEvent, UtilsEnum.Module.SMPP_SERVER,
                    UtilsEnum.MessageType.DELIVER, UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
            sendProxyResponse(kafkaTemplate, deliverSmEvent, UtilsEnum.CdrStatus.FAILED);
            log.error("Error on process deliverSm {} ex -> {}", deliverSmEvent, e.getMessage());
        }
    }

    private static int handleException(Exception exception, MessageEvent submitSmEvent) {
        String specificExceptionType = exception.getClass().getSimpleName();
        String message = "Failed on send deliver_sm with id {} and messageId {} due to " + specificExceptionType;
        log.warn(message, submitSmEvent.getId(), submitSmEvent.getMessageId(), exception);
        return switch (exception) {
            case PDUException ignored -> ErrorCodes.PDU_EXCEPTION_ERROR;
            case ResponseTimeoutException ignored -> ErrorCodes.TIMEOUT_ERROR;
            case InvalidResponseException ignored -> ErrorCodes.INVALID_RESPONSE_EXCEPTION_ERROR;
            case NegativeResponseException negativeResponseException -> negativeResponseException.getCommandStatus();
            case IOException ignored -> ErrorCodes.IO_EXCEPTION_ERROR;
            default -> ErrorCodes.SYSTEM_ERROR;
        };
    }

    private static void replaceOptionalParameter(
            List<UtilsRecords.OptionalParameter> optionalParameters,
            MessageEvent deliverSmEvent) {
        UtilsRecords.OptionalParameter currentRecord = null;
        UtilsRecords.OptionalParameter newRecord = null;
        if (!optionalParameters.isEmpty()) {
            for (UtilsRecords.OptionalParameter op : optionalParameters) {
                if (op.tag() == 30) {
                    currentRecord = op;
                    newRecord = new UtilsRecords.OptionalParameter(op.tag(), deliverSmEvent.getDeliverSmServerId());
                    break;
                }
            }
        }
        if (Objects.nonNull(newRecord)) {
            optionalParameters.remove(currentRecord);
            optionalParameters.add(newRecord);
        }
    }

    private static void removeOptionalParameter(
            List<UtilsRecords.OptionalParameter> optionalParameters) {
        optionalParameters.removeIf(p -> p.tag() == 30 || p.tag() == 1059 || p.tag() == 1063);
    }

    public static boolean isValidDataCoding(int dataCoding) {
        return validDataCodings.contains(dataCoding);
    }

    private static ESMClass getEsmClass(Integer esmClass, boolean containsUdh) {
        if (Objects.nonNull(esmClass)) {
            var esmeClass = new ESMClass(esmClass);
            esmeClass.setSpecificFeature(containsUdh ? GSMSpecificFeature.UDHI : GSMSpecificFeature.DEFAULT);
            return esmeClass;
        }
        return new ESMClass(MessageMode.DEFAULT, MessageType.SMSC_DEL_RECEIPT, GSMSpecificFeature.DEFAULT);
    }

    public static void publishPendingDlrFailureCdr(MessageEvent event, KafkaTemplate<String, String> kafkaTemplate) {
        if (event == null) return;
        event.setErrorCode(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION);
        cdrProcessor.publishCdr(event, UtilsEnum.Module.SMPP_SERVER,
                UtilsEnum.MessageType.DELIVER, UtilsEnum.CdrStatus.FAILED, kafkaTemplate);
        sendProxyResponse(kafkaTemplate, event, UtilsEnum.CdrStatus.FAILED);
        log.warn("Published pending DLR failure CDR for messageId {}", event.getMessageId());
    }

    public static void sendProxyResponse(KafkaTemplate<String, String> kafkaTemplate, MessageEvent event, UtilsEnum.CdrStatus cdrStatus) {
        if (event.isUseProxy()) {
            boolean completedWithError = false;
            String errorMsg = "";

            if (cdrStatus.equals(UtilsEnum.CdrStatus.FAILED)) {
                completedWithError = true;
                errorMsg = ErrorCodes.getErrorDescription(UtilsEnum.Module.SMPP_SERVER, event.getErrorCode(), null);
            }

            UtilsRecords.HttpProxyResponse  httpProxyResponse = new UtilsRecords.HttpProxyResponse(event.getMessageId(), completedWithError,  event.getErrorCode(), errorMsg, event.getDeliverSmServerId());

            kafkaTemplate.send(KafkaTopicsConstants.HTTP_PROXY_TOPIC, httpProxyResponse.toString());
        }
    }
}
