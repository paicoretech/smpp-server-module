package com.paicbd.module.e2e;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.EncodingUtils;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.bean.AlertNotification;
import org.jsmpp.bean.DataSm;
import org.jsmpp.bean.DeliverSm;
import org.jsmpp.bean.EnquireLink;
import org.jsmpp.bean.MessageRequest;
import org.jsmpp.bean.SubmitSm;
import org.jsmpp.session.DataSmResult;
import org.jsmpp.session.MessageReceiverListener;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.Session;
import org.jsmpp.session.SubmitSmResult;
import org.jsmpp.util.InvalidDeliveryReceiptException;

import java.util.Objects;

@Setter
@Slf4j
public class MessageReceiverListenerImplMock implements MessageReceiverListener {
    private final ServiceProvider serviceProvider;

    public MessageReceiverListenerImplMock(ServiceProvider serviceProvider) {
        this.serviceProvider = serviceProvider;
    }

    @Override
    public void onAcceptDeliverSm(final DeliverSm deliverSm) {
        log.info("reading deliverSm");
        try {
            log.info(getDeliverSmEvent(deliverSm).toString());
        } catch (InvalidDeliveryReceiptException e) {
            throw new RuntimeException(e);
        }
    }

    private MessageEvent getDeliverSmEvent(DeliverSm deliverSm) throws InvalidDeliveryReceiptException {
        int encodingType = 0;
        String decodedMessage = EncodingUtils.decodeMessage(deliverSm.getShortMessage(), encodingType);

        MessageEvent deliverSmEvent = new MessageEvent();
        deliverSmEvent.setId(System.currentTimeMillis() + "-" + System.nanoTime());
        deliverSmEvent.setRegisteredDelivery((int) deliverSm.getRegisteredDelivery());
        this.setDataToMessageEvent(deliverSmEvent, deliverSm);
        deliverSmEvent.setShortMessage(decodedMessage);
        deliverSmEvent.setDelReceipt(deliverSm.getShortMessageAsDeliveryReceipt().toString());

        deliverSmEvent.setSystemId(this.serviceProvider.getSystemId());
        deliverSmEvent.setRegisteredDelivery((int) deliverSm.getRegisteredDelivery());


        return deliverSmEvent;
    }

    private void setDataToMessageEvent(MessageEvent messageEvent, MessageRequest messageRequest) {
        messageEvent.setCommandStatus(messageRequest.getCommandStatus());
        messageEvent.setSequenceNumber(messageRequest.getSequenceNumber());
        messageEvent.setSourceAddrTon((int) messageRequest.getSourceAddrTon());
        messageEvent.setSourceAddrNpi((int) messageRequest.getSourceAddrNpi());
        messageEvent.setSourceAddr(messageRequest.getSourceAddr());
        messageEvent.setDestAddrTon((int) messageRequest.getDestAddrTon());
        messageEvent.setDestAddrNpi((int) messageRequest.getDestAddrNpi());
        messageEvent.setDestinationAddr(messageRequest.getDestAddress());
        messageEvent.setEsmClass((int) messageRequest.getEsmClass());
        long validityPeriod = Objects.isNull(messageRequest.getValidityPeriod()) ? 0
                : Converter.smppValidityPeriodToSeconds(messageRequest.getValidityPeriod());
        messageEvent.setValidityPeriod(validityPeriod);
        messageEvent.setDataCoding((int) messageRequest.getDataCoding());
        messageEvent.setSmDefaultMsgId(messageRequest.getSmDefaultMsgId());
    }

    @Override
    public void onAcceptEnquireLink(EnquireLink enquireLink, Session source) {
        MessageReceiverListener.super.onAcceptEnquireLink(enquireLink, source);
    }

    @Override
    public void onAcceptAlertNotification(AlertNotification alertNotification) {
        log.info("onAcceptAlertNotification: {} {}", alertNotification.getSourceAddr(), alertNotification.getEsmeAddr());
    }

    @Override
    public SubmitSmResult onAcceptSubmitSm(SubmitSm submitSm, SMPPSession smppSession) {
        return null;
    }

    @Override
    public DataSmResult onAcceptDataSm(DataSm dataSm, Session session) {
        return null;
    }
}
