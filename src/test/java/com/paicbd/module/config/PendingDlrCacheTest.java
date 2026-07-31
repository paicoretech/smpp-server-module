package com.paicbd.module.config;

import com.paicbd.module.utils.AppProperties;
import com.paicbd.smsc.dto.MessageEvent;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.ErrorCodes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_SECOND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PendingDlrCacheTest {

    @Mock
    AppProperties appProperties;

    @Mock
    KafkaTemplate<String, String> kafkaTemplate;

    @Test
    @DisplayName("put when cache is enabled then stores event so drainAll returns it")
    void putWhenCacheEnabledThenStoresEventSoDrainAllReturnsIt() {
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        PendingDlrCache cache = new PendingDlrCache(appProperties, kafkaTemplate);

        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        MessageEvent event = MessageEvent.builder()
                .messageId(messageId)
                .destNetworkId(1)
                .sourceAddr("50510201020")
                .destinationAddr("50582368999")
                .build();

        cache.put(event);

        var result = cache.drainAll();
        assertEquals(1, result.size());
        assertTrue(result.iterator().next().contains(messageId));
    }

    @Test
    @DisplayName("put when cache is disabled then publishes failure CDR and does not store the event")
    void putWhenCacheDisabledThenPublishesFailureCdrAndDoesNotCache() {
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(false);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        PendingDlrCache cache = new PendingDlrCache(appProperties, kafkaTemplate);

        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        MessageEvent event = MessageEvent.builder()
                .id(messageId)
                .messageId(messageId)
                .destNetworkId(1)
                .build();

        cache.put(event);

        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(anyString(), kafkaCaptor.capture());
        UtilsRecords.Cdr cdr = Converter.stringToObject(kafkaCaptor.getValue(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals("FAILED", cdr.status());
        assertEquals(String.valueOf(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION), cdr.statusCode());
        assertEquals(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION, event.getErrorCode());
        assertTrue(cache.drainAll().isEmpty());
    }

    @Test
    @DisplayName("drainAll returns all stored entries and leaves the cache empty on subsequent call")
    void drainAllReturnsAllEntriesAndLeavesEmpty() {
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        PendingDlrCache cache = new PendingDlrCache(appProperties, kafkaTemplate);

        String messageId1 = System.currentTimeMillis() + "-" + System.nanoTime();
        String messageId2 = System.currentTimeMillis() + "-" + System.nanoTime();
        cache.put(MessageEvent.builder().messageId(messageId1).destNetworkId(1).build());
        cache.put(MessageEvent.builder().messageId(messageId2).destNetworkId(1).build());

        var firstDrain = cache.drainAll();
        assertEquals(2, firstDrain.size());

        var secondDrain = cache.drainAll();
        assertTrue(secondDrain.isEmpty());
    }

    @Test
    @DisplayName("drainAll uses explicit invalidation so the removal listener does not fire a CDR")
    void drainAllDoesNotTriggerRemovalListenerCdr() {
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        PendingDlrCache cache = new PendingDlrCache(appProperties, kafkaTemplate);

        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        cache.put(MessageEvent.builder().messageId(messageId).destNetworkId(1).build());

        cache.drainAll();
        cache.cleanUp();

        verifyNoMoreInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("publishPendingFailureCdrsAndClear publishes a failure CDR for each pending entry and empties the cache")
    void publishPendingFailureCdrsAndClearPublishesCdrForEachPendingEntry() {
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(100);
        PendingDlrCache cache = new PendingDlrCache(appProperties, kafkaTemplate);

        String messageId1 = System.currentTimeMillis() + "-" + System.nanoTime();
        String messageId2 = System.currentTimeMillis() + "-" + System.nanoTime();
        cache.put(MessageEvent.builder().id(messageId1).messageId(messageId1).destNetworkId(1)
                .sourceAddr("50510201020").destinationAddr("50582368999").build());
        cache.put(MessageEvent.builder().id(messageId2).messageId(messageId2).destNetworkId(1)
                .sourceAddr("50510201020").destinationAddr("50582368999").build());

        cache.publishPendingFailureCdrsAndClear();

        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, times(2)).send(anyString(), kafkaCaptor.capture());
        kafkaCaptor.getAllValues().forEach(payload -> {
            UtilsRecords.Cdr cdr = Converter.stringToObject(payload, UtilsRecords.Cdr.class);
            assertNotNull(cdr);
            assertEquals("FAILED", cdr.status());
            assertEquals(String.valueOf(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION), cdr.statusCode());
        });
        assertTrue(cache.drainAll().isEmpty());
    }

    @Test
    @DisplayName("removal listener publishes a failure CDR when an entry is evicted due to maximum size")
    void removalListenerPublishesFailureCdrOnSizeEviction() {
        int maxSize = 2;
        when(appProperties.isPendingDlrCacheEnabled()).thenReturn(true);
        when(appProperties.getPendingDlrCacheMaximumSize()).thenReturn(maxSize);
        PendingDlrCache cache = new PendingDlrCache(appProperties, kafkaTemplate);

        for (int i = 0; i < maxSize + 2; i++) {
            String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
            cache.put(MessageEvent.builder()
                    .id(messageId)
                    .messageId(messageId)
                    .destNetworkId(1)
                    .sourceAddr("50510201020")
                    .sourceAddrTon(1)
                    .sourceAddrNpi(1)
                    .destinationAddr("50582368999")
                    .destAddrTon(1)
                    .destAddrNpi(1)
                    .esmClass(0)
                    .dataCoding(0)
                    .build());
        }
        cache.cleanUp();
        await().atMost(ONE_SECOND).untilAsserted(() ->
                verify(kafkaTemplate, atLeastOnce()).send(anyString(), anyString()));

        ArgumentCaptor<String> kafkaCaptor = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate, atLeastOnce()).send(anyString(), kafkaCaptor.capture());
        UtilsRecords.Cdr cdr = Converter.stringToObject(kafkaCaptor.getAllValues().getFirst(), UtilsRecords.Cdr.class);
        assertNotNull(cdr);
        assertEquals("FAILED", cdr.status());
        assertEquals(String.valueOf(ErrorCodes.SMPP_CONNECTION_UNAVAILABLE_EVICTION), cdr.statusCode());
    }
}
