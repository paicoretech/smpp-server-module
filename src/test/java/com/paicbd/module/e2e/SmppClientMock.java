package com.paicbd.module.e2e;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.SmppUtils;
import com.paicbd.smsc.utils.UtilsEnum;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.GSMSpecificFeature;
import org.jsmpp.bean.OptionalParameter;
import org.jsmpp.bean.RegisteredDelivery;
import org.jsmpp.session.BindParameter;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.SubmitSmResult;

import java.util.Objects;

import static com.paicbd.smsc.utils.EncodingUtils.DCS_0;
import static com.paicbd.smsc.utils.EncodingUtils.DCS_3;
import static com.paicbd.smsc.utils.EncodingUtils.DCS_8;

@Slf4j
public class SmppClientMock {
    private final String host;
    private final int port;

    public SmppClientMock(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public SMPPSession createAndBindSmppSession(ServiceProvider serviceProvider) {
        SMPPSession smppSession = new SMPPSession();
        smppSession.addSessionStateListener(new SessionStateListenerImplMock());
        smppSession.setMessageReceiverListener(new MessageReceiverListenerImplMock(serviceProvider));
        smppSession.setTransactionTimer(serviceProvider.getPduTimeout());
        smppSession.setEnquireLinkTimer(serviceProvider.getEnquireLinkPeriod());
        smppSession.setPduProcessorDegree(5);
        smppSession.setQueueCapacity(5);

        try {
            smppSession.connectAndBind(
                    this.host,
                    this.port,
                    new BindParameter(
                            UtilsEnum.getBindType(serviceProvider.getBindType()),
                            serviceProvider.getSystemId(),
                            serviceProvider.getPassword(),
                            serviceProvider.getSystemType(),
                            UtilsEnum.getTypeOfNumber(serviceProvider.getAddressTon()),
                            UtilsEnum.getNumberingPlanIndicator(serviceProvider.getAddressNpi()),
                            serviceProvider.getAddressRange(),
                            UtilsEnum.getInterfaceVersion(serviceProvider.getInterfaceVersion())
                    ));

            return smppSession;
        } catch (Exception e) {
            log.error("Error while connecting to Service provider: {}", serviceProvider.getSystemId());
        }

        return null;
    }

    public SubmitSmResult sendSubmit(MessageEvent submitSmEvent, SMPPSession smppSession) {
        try {
            byte[] udh = getUdhiBytes(submitSmEvent);

            int encodingType = determineEncodingType(submitSmEvent.getDataCoding());
            DataCoding dataCoding = EncodingUtils.getDataCoding(submitSmEvent.getDataCoding());
            byte[] encodedShortMessage = EncodingUtils.encodeMessage(submitSmEvent.getShortMessage(), encodingType);

            // is UDHI
            if (udh != null) {
                byte[] fullMessage = new byte[udh.length + encodedShortMessage.length];
                System.arraycopy(udh, 0, fullMessage, 0, udh.length);  // Copiar el UDH
                System.arraycopy(encodedShortMessage, 0, fullMessage, udh.length, encodedShortMessage.length);  // Copiar el mensaje codificado
                encodedShortMessage = fullMessage;
            }

            return smppSession.submitShortMessage(
                    null,
                    UtilsEnum.getTypeOfNumber(submitSmEvent.getSourceAddrTon()), UtilsEnum.getNumberingPlanIndicator(submitSmEvent.getSourceAddrNpi()), submitSmEvent.getSourceAddr(),
                    UtilsEnum.getTypeOfNumber(submitSmEvent.getDestAddrTon()), UtilsEnum.getNumberingPlanIndicator(submitSmEvent.getDestAddrNpi()), submitSmEvent.getDestinationAddr(),
                    new ESMClass(submitSmEvent.getEsmClass()),
                    (byte) 0,
                    (byte) 0,
                    null,
                    submitSmEvent.getStringValidityPeriod(),
                    new RegisteredDelivery(submitSmEvent.getRegisteredDelivery()),
                    (byte) 0,
                    dataCoding,
                    (byte) 0,
                    encodedShortMessage,
                    Objects.nonNull(submitSmEvent.getOptionalParameters()) ? SmppUtils.getTLV(submitSmEvent) : new OptionalParameter[0]
            );
        } catch (Exception e) {
            log.error("Error sending submit in smpp client mock", e);
            return null;
        }
    }

    private byte[] getUdhiBytes(MessageEvent submitSmEvent) {
        byte messageType = convertIntegerToByte(submitSmEvent.getEsmClass());
        boolean isGSMSpecificFeatureDefault = GSMSpecificFeature.DEFAULT.containedIn(messageType);

        byte[] udh = null;
        if (!isGSMSpecificFeatureDefault) {
            byte reference = Byte.parseByte(submitSmEvent.getMsgReferenceNumber());
            byte totalSegment = convertIntegerToByte(submitSmEvent.getTotalSegment());
            byte segment = convertIntegerToByte(submitSmEvent.getSegmentSequence());

            udh = new byte[]{
                    0x05,
                    (byte) 0x00,
                    (byte) 0x03,
                    reference,
                    totalSegment,
                    segment
            };
        }
        return udh;
    }

    private int determineEncodingType(int encodingType) {
        return switch (encodingType) {
            case DCS_0 -> 0;
            case DCS_8 -> 2;
            case DCS_3 -> 3;
            default ->
                    throw new IllegalStateException("Unexpected value when determining encoding type: " + encodingType);
        };
    }

    private byte convertIntegerToByte(int integerValue) {
        return (byte) integerValue;
    }
}
