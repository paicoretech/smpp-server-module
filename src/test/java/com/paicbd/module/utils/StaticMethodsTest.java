package com.paicbd.module.utils;

import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.Udh;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.EncodingUtils;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.SmppUtils;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.InvalidResponseException;
import org.jsmpp.PDUException;
import org.jsmpp.bean.DataCoding;
import org.jsmpp.bean.ESMClass;
import org.jsmpp.bean.GSMSpecificFeature;
import org.jsmpp.bean.MessageMode;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.paicbd.smsc.utils.EncodingUtils.DCS_0;
import static com.paicbd.smsc.utils.EncodingUtils.DCS_3;
import static com.paicbd.smsc.utils.EncodingUtils.DCS_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
@Slf4j
class StaticMethodsTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private SMPPServerSession serverSession;

    @ParameterizedTest
    @MethodSource("sendDeliverSmParameters")
    @DisplayName("""
            sendDeliverSm when delReceipt is not null then the method is executed
            replacing optional parameters if needed, parsing the EsmClass and creating the proper CDR entry.
            """)
    void sendDeliverSm(Integer esmClass,
                       Set<Udh> udhRaw,
                       ESMClass esmClassExpected,
                       List<UtilsRecords.OptionalParameter> optionalParameters,
                       Integer dataCoding,
                       String delReceipt,
                       boolean forceNullMessageBytes,
                       boolean nullShortMessage)
            throws ResponseTimeoutException, PDUException, IOException,
            InvalidResponseException, NegativeResponseException {

        GeneralSettings generalSettings = GeneralSettings.builder()
                .encodingGsm7(EncodingUtils.UTF8)
                .encodingUcs2(EncodingUtils.UCS2)
                .encodingIso88591(EncodingUtils.ISO88591)
                .build();

        byte[] shortMessage = "Test Message".getBytes();
        if (!udhRaw.isEmpty()) {
            byte[] udhBytes = new byte[]{
                    0x00, 0x03, 0x01, 0x02, 0x01
            };
            shortMessage = EncodingUtils.prepend(udhBytes, shortMessage);
        }

        MessageEvent messageEvent = MessageEvent.builder()
                .messageId("1")
                .id("1719421854353-11028072268459")
                .shortMessage("Test Message")
                .registeredDelivery(0)
                .delReceipt(delReceipt)
                .dataCoding(dataCoding)
                .esmClass(esmClass)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .sourceAddr("1234")
                .destinationAddr("5678")
                .udhLength(udhRaw.isEmpty() ? 0 : 5)
                .udhRaw(udhRaw)
                .messageBytes(shortMessage)
                .udhBytes(new byte[]{0x00, 0x03, 0x01, 0x02, 0x01})
                .optionalParameters(optionalParameters)
                .build();

        if (forceNullMessageBytes) {
            messageEvent.setMessageBytes(null);
            messageEvent.setShortMessage(nullShortMessage ? null : "Test Message");
        }

        boolean isReplacementOptionalParameterTest = esmClass != null && esmClass == 3
                && (optionalParameters != null && !optionalParameters.isEmpty());

        if (isReplacementOptionalParameterTest) {
            OptionalParameter messageReceiptId = new OptionalParameter.Receipted_message_id("1");
            OptionalParameter[] optionalParameterList = new OptionalParameter[]{messageReceiptId};
            SmppUtils.setTLV(messageEvent, optionalParameterList);
        }

        ArgumentCaptor<TypeOfNumber> sourceAddressTON = ArgumentCaptor.forClass(TypeOfNumber.class);
        ArgumentCaptor<NumberingPlanIndicator> sourceAddressNPI = ArgumentCaptor.forClass(NumberingPlanIndicator.class);
        ArgumentCaptor<String> sourceAddress = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<TypeOfNumber> destinationAddressTON = ArgumentCaptor.forClass(TypeOfNumber.class);
        ArgumentCaptor<NumberingPlanIndicator> destinationAddressNPI = ArgumentCaptor.forClass(NumberingPlanIndicator.class);
        ArgumentCaptor<String> destinationAddress = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<ESMClass> esmClassCaptor = ArgumentCaptor.forClass(ESMClass.class);
        ArgumentCaptor<DataCoding> dataCodingCaptor = ArgumentCaptor.forClass(DataCoding.class);
        ArgumentCaptor<byte[]> shortMessageCaptor = ArgumentCaptor.forClass(byte[].class);
        ArgumentCaptor<OptionalParameter[]> optionalParametersCaptor = ArgumentCaptor.forClass(OptionalParameter[].class);

        ArgumentCaptor<String> serviceTypeCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Byte> protocolIdCaptor = ArgumentCaptor.forClass(Byte.class);
        ArgumentCaptor<Byte> priorityFlagCaptor = ArgumentCaptor.forClass(Byte.class);
        ArgumentCaptor<RegisteredDelivery> registeredDeliveryCaptor = ArgumentCaptor.forClass(RegisteredDelivery.class);

        SMPPServerSession session = mock(SMPPServerSession.class);

        StaticMethods.sendDeliverSm(session, messageEvent, true, generalSettings, kafkaTemplate);

        verify(session).deliverShortMessage(
                serviceTypeCaptor.capture(),
                sourceAddressTON.capture(),
                sourceAddressNPI.capture(),
                sourceAddress.capture(),
                destinationAddressTON.capture(),
                destinationAddressNPI.capture(),
                destinationAddress.capture(),
                esmClassCaptor.capture(),
                protocolIdCaptor.capture(),
                priorityFlagCaptor.capture(),
                registeredDeliveryCaptor.capture(),
                dataCodingCaptor.capture(),
                shortMessageCaptor.capture(),
                optionalParametersCaptor.capture()
        );

        ESMClass esm = esmClassCaptor.getValue();
        assertEquals(esmClassExpected.value(), esm.value());
    }

    static Stream<Arguments> sendDeliverSmParameters() {
        String delReceipt = "id:1 sub:001 dlvrd:001 submit date:2101010000 done date:2101010000 stat:DELIVRD err:000 text:Test Message";

        ESMClass esmDefaultDefaultUdh = new ESMClass(MessageMode.DEFAULT, MessageType.DEFAULT, GSMSpecificFeature.UDHI);
        ESMClass esmDefault = new ESMClass(MessageMode.DEFAULT, MessageType.DEFAULT, GSMSpecificFeature.DEFAULT);
        ESMClass esmDefaultReceiptDefault = new ESMClass(MessageMode.DEFAULT, MessageType.SMSC_DEL_RECEIPT, GSMSpecificFeature.DEFAULT);
        ESMClass esmStoreDefaultUdh = new ESMClass(MessageMode.STORE_AND_FORWARD, MessageType.DEFAULT, GSMSpecificFeature.UDHI);

        UtilsRecords.OptionalParameter unknownOptionalParameter = new UtilsRecords.OptionalParameter((short) 31, "1");
        UtilsRecords.OptionalParameter receiptedMessageId = new UtilsRecords.OptionalParameter((short) 30, "1");

        return Stream.of(
                Arguments.of(0, Set.of(new Udh("00","0003010201")), esmDefaultDefaultUdh, null, 0, delReceipt, false, false),
                Arguments.of(0, Set.of(),                           esmDefault,            null, 0, delReceipt, false, false),
                Arguments.of(null, Set.of(),                        esmDefaultReceiptDefault, null, 0, delReceipt, false, false),
                Arguments.of(0, Set.of(new Udh("00","0003010201")), esmDefaultDefaultUdh, List.of(unknownOptionalParameter), 0, delReceipt, false, false),
                Arguments.of(null, Set.of(),                        esmDefaultReceiptDefault, null, null, delReceipt, false, false),
                Arguments.of(3, Set.of(new Udh("00","0003010201")), esmStoreDefaultUdh,   List.of(receiptedMessageId), 0, delReceipt, false, false),
                Arguments.of(3, Set.of(new Udh("00","0003010201")), esmStoreDefaultUdh,   new ArrayList<>(), 0, null, false, false),
                Arguments.of(0, Set.of(),                           esmDefault,            null, 0, delReceipt, true,  false),
                Arguments.of(0, Set.of(),                           esmDefault,            null, 0, delReceipt, true,  true)
        );
    }

    @ParameterizedTest
    @ValueSource(ints = {DCS_0, DCS_8, DCS_3})
    @DisplayName("isValidDataCoding when data coding is valid then return true")
    void isValidDataCodingWhenValidThenReturnTrue(int dataCoding) {
        assertTrue(StaticMethods.isValidDataCoding(dataCoding));
    }

    @ParameterizedTest
    @ValueSource(ints = {11, 98, 67, 87})
    @DisplayName("isValidDataCoding when data coding is not valid then return false")
    void isValidDataCodingWhenInValidThenReturnFalse(int dataCoding) {
        assertFalse(StaticMethods.isValidDataCoding(dataCoding));
    }

    @Test
    @DisplayName("sendDeliverSm should execute catch block and handleException when an exception occurs")
    void sendDeliverSmShouldHandleException() {
        GeneralSettings generalSettings = GeneralSettings.builder()
                .encodingGsm7(EncodingUtils.UTF8)
                .encodingUcs2(EncodingUtils.UCS2)
                .encodingIso88591(EncodingUtils.ISO88591)
                .build();

        MessageEvent messageEvent = MessageEvent.builder()
                .id("1")
                .messageId("1719421854353-11028072268459")
                .sourceAddr("1234")
                .destinationAddr("5678")
                .esmClass(3)
                .dataCoding(0)
                .shortMessage("Test Message")
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .build();

        try {
            Mockito.doThrow(new IOException("Simulated exception"))
                    .when(serverSession)
                    .deliverShortMessage(
                            anyString(), // serviceType
                            any(TypeOfNumber.class), any(NumberingPlanIndicator.class), anyString(), // Source address
                            any(TypeOfNumber.class), any(NumberingPlanIndicator.class), anyString(), // Destination address
                            any(ESMClass.class), anyByte(), anyByte(), // ProtocolId, PriorityFlag
                            any(RegisteredDelivery.class), any(DataCoding.class), any(byte[].class), // Message Content
                            any(OptionalParameter[].class) // Optional Parameters
                    );
        } catch (Exception e) {
            log.error("Simulated exception", e);
        }

        StaticMethods.sendDeliverSm(serverSession, messageEvent, true, generalSettings, kafkaTemplate);

        assertEquals(ErrorCodes.SYSTEM_ERROR, messageEvent.getErrorCode(), "The errorCode must match SYSTEM_ERROR");

        verify(kafkaTemplate, times(1)).send(anyString(), anyString());
    }

    @ParameterizedTest
    @MethodSource("handleExceptionParameters")
    @DisplayName("handleException should map exceptions to the expected ErrorCodes")
    void handleExceptionShouldMapProperly(Exception exception, int expectedCode) throws Exception {
        MessageEvent dummy = MessageEvent.builder()
                .id("x")
                .messageId("y")
                .build();

        Method m = StaticMethods.class.getDeclaredMethod("handleException", Exception.class, MessageEvent.class);
        m.setAccessible(true);
        int code = (int) m.invoke(null, exception, dummy);

        assertEquals(expectedCode, code);
    }

    static Stream<Arguments> handleExceptionParameters() {
        NegativeResponseException neg = new NegativeResponseException(0x25);

        return Stream.of(
                Arguments.of(new PDUException("pdu"), ErrorCodes.PDU_EXCEPTION_ERROR),
                Arguments.of(new ResponseTimeoutException("timeout"), ErrorCodes.TIMEOUT_ERROR),
                Arguments.of(new InvalidResponseException("invalid"), ErrorCodes.INVALID_RESPONSE_EXCEPTION_ERROR),
                Arguments.of(neg, 0x25),
                Arguments.of(new IOException("io"), ErrorCodes.IO_EXCEPTION_ERROR),
                Arguments.of(new RuntimeException("other"), ErrorCodes.SYSTEM_ERROR)
        );
    }
}