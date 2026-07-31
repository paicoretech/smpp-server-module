package com.paicbd.module.utils;

import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.ErrorCodes;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.SMPPSession;
import org.jsmpp.session.Session;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import com.paicbd.smsc.utils.RedisManager;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SpSessionTest {

    @Mock
    RedisManager redisManager;

    @Mock
    ServiceProvider currentServiceProvider;

    @Mock
    Session mockSession;

    @Mock
    AppProperties appProperties;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    SpSession spSession;

    @BeforeEach
    void setUp() throws Exception {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        spSession = new SpSession(redisManager, currentServiceProvider, appProperties, kafkaTemplate);
        setUpSessions();
    }

    private void setUpSessions() throws Exception {
        Field field = spSession.getClass().getDeclaredField("currentSmppSessions");
        field.setAccessible(true);
        field.set(spSession, new ArrayList<>(Collections.singletonList(mockSession)));
    }

    @Test
    @DisplayName("constructor creates a non-null PendingDlrCache accessible via getter")
    void constructorCreatesPendingDlrCacheAccessibleViaGetter() {
        assertNotNull(spSession.getPendingDlrCache());
    }

    @Test
    @DisplayName("init when current service provider has available credit then hasAvailableCredit is set")
    void initWhenCurrentServiceProviderHasAvailableCreditThenHasAvailableCreditIsSet() {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .password("password")
                .systemType("smpp")
                .interfaceVersion("IF_50")
                .maxBinds(10)
                .currentBindsCount(0)
                .addressTon(1)
                .addressNpi(1)
                .protocol("SMPP")
                .hasAvailableCredit(true)
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .enabled(1)
                .build();
        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);
        spSession.init();
        assertEquals(sp.getHasAvailableCredit(), spSession.hasAvailableCredit());
    }

    @Test
    @DisplayName("updateRedis when data is not empty then executes hset on redisManager")
    void updateRedisWhenDataIsNotEmptyThenExecuteSet() {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .password("password")
                .systemType("smpp")
                .interfaceVersion("IF_50")
                .maxBinds(10)
                .currentBindsCount(0)
                .addressTon(1)
                .addressNpi(1)
                .protocol("SMPP")
                .hasAvailableCredit(true)
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .enabled(1)
                .build();

        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);
        spSession.updateRedis();
        verify(redisManager).hset(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME,
                String.valueOf(sp.getNetworkId()), sp.toString());
    }

    @Test
    @DisplayName("updateCurrentServiceProvider when enabled updates all fields and writes to Redis")
    void updateCurrentServiceProviderWhenIsEnabledThenUpdateRedis() {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .password("password")
                .systemType("smpp")
                .interfaceVersion("IF_50")
                .maxBinds(5)
                .currentBindsCount(0)
                .addressTon(1)
                .addressNpi(1)
                .protocol("SMPP")
                .hasAvailableCredit(true)
                .enquireLinkPeriod(0)
                .pduTimeout(0)
                .enabled(0)
                .status(Constants.STOPPED)
                .build();

        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);

        ServiceProvider updates = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .password("password")
                .systemType("smpp")
                .interfaceVersion("IF_50")
                .maxBinds(10)
                .currentBindsCount(0)
                .addressTon(1)
                .addressNpi(1)
                .protocol("SMPP")
                .hasAvailableCredit(false)
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .enabled(1)
                .build();

        SpSession spSessionSpy = spy(spSession);
        spSessionSpy.updateCurrentServiceProvider(updates);
        assertFalse(spSession.updateCurrentServiceProvider(updates));
        verify(spSessionSpy).updateRedis();
        assertEquals(updates.toString(), spSession.getCurrentServiceProvider().toString());
    }

    @Test
    @DisplayName("updateCurrentServiceProvider when not enabled then unbinds all sessions and writes STOPPED to Redis")
    void updateCurrentServiceProviderWhenIsNotEnabledThenUnbindAndClose() throws Exception {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .password("password")
                .systemType("smpp")
                .interfaceVersion("IF_50")
                .maxBinds(5)
                .binds(new ArrayList<>())
                .currentBindsCount(0)
                .addressTon(1)
                .addressNpi(1)
                .protocol("SMPP")
                .hasAvailableCredit(true)
                .enquireLinkPeriod(0)
                .pduTimeout(0)
                .enabled(1)
                .status(Constants.STARTED)
                .build();

        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);
        setUpSessions();

        ServiceProvider updates = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .password("password")
                .systemType("smpp")
                .interfaceVersion("IF_50")
                .maxBinds(10)
                .binds(new ArrayList<>())
                .currentBindsCount(0)
                .addressTon(1)
                .addressNpi(1)
                .protocol("SMPP")
                .hasAvailableCredit(false)
                .enquireLinkPeriod(5000)
                .pduTimeout(5000)
                .enabled(0)
                .status(Constants.STOPPED)
                .build();

        SpSession spSessionSpy = spy(spSession);
        spSessionSpy.updateCurrentServiceProvider(updates);
        verify(spSessionSpy).updateRedis();
        verify(mockSession).unbindAndClose();
        assertEquals(updates.toString(), spSession.getCurrentServiceProvider().toString());
    }

    @Test
    @DisplayName("autoDestroy publishes failure CDRs for all pending cache entries then updates Redis")
    void autoDestroyPublishesCdrsForPendingEntriesAndUpdatesRedis() {
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .enabled(1)
                .build();
        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);

        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        spSession.getPendingDlrCache().put(MessageEvent.builder()
                .id(messageId)
                .messageId(messageId)
                .sourceAddr("50510201020")
                .destinationAddr("50582368999")
                .destNetworkId(sp.getNetworkId())
                .build());

        spSession.autoDestroy();

        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), kafkaCaptor.capture());
        UtilsRecords.Cdr cdr = Converter.stringToObject(kafkaCaptor.getAllValues().getFirst(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals("FAILED", cdr.status());
        assertEquals(String.valueOf(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION), cdr.statusCode());
        verify(redisManager).hset(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME,
                String.valueOf(sp.getNetworkId()), sp.toString());
    }

    @Test
    @DisplayName("getNextRoundRobinSession when no sessions are present returns null")
    void getNextRoundRobinSessionWhenNoSessionThenReturnNull() {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .maxBinds(10)
                .binds(new ArrayList<>())
                .hasAvailableCredit(false)
                .enabled(0)
                .status(Constants.STOPPED)
                .build();
        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);
        assertNull(spSession.getNextRoundRobinSession());
    }

    @Test
    @DisplayName("getNextRoundRobinSession cycles through receivable sessions in round-robin order")
    void getNextRoundRobinSessionWhenSessionsPresentThenGetNextInOrder() {
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        ServiceProvider sp = ServiceProvider.builder()
                .networkId(10)
                .systemId("smpp_test")
                .maxBinds(10)
                .binds(new ArrayList<>())
                .hasAvailableCredit(false)
                .enabled(0)
                .status(Constants.STOPPED)
                .bindType("TRANSCEIVER")
                .build();
        spSession = new SpSession(redisManager, sp, appProperties, kafkaTemplate);
        SMPPSession sessionTest = Mockito.mock(SMPPSession.class);
        when(sessionTest.getSessionState()).thenReturn(SessionState.BOUND_TRX);
        spSession.getCurrentSmppSessions().add(sessionTest);
        spSession.getCurrentSmppSessions().add(sessionTest);
        assertEquals(sessionTest, spSession.getNextRoundRobinSession());
        spSession.getCurrentSmppSessions().remove(1);
        assertEquals(sessionTest, spSession.getNextRoundRobinSession());
    }
}
