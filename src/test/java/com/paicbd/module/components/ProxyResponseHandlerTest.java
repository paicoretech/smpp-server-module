package com.paicbd.module.components;

import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Field;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class ProxyResponseHandlerTest {

    @InjectMocks
    ProxyResponseHandler handler;

    @Test
    @DisplayName("waitForResponse returns the response completed by completeFuture")
    void registerCompleteAndWaitReturnsResponse() {
        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        UtilsRecords.HttpProxyResponse expected = new UtilsRecords.HttpProxyResponse(messageId, false, 0, "", null);

        handler.register(messageId);
        Thread.startVirtualThread(() -> handler.completeFuture(messageId, expected));

        UtilsRecords.HttpProxyResponse actual = handler.waitForResponse(messageId, 2000L);
        assertEquals(expected, actual);
    }

    @Test
    @DisplayName("waitForResponse returns null when the confirmation does not arrive before the timeout")
    void waitForResponseTimesOutReturnsNull() {
        handler.register("msg-timeout");
        assertNull(handler.waitForResponse("msg-timeout", 50L));
    }

    @Test
    @DisplayName("waitForResponse returns null when no future was registered")
    void waitForResponseWithoutRegistrationReturnsNull() {
        assertNull(handler.waitForResponse("unknown", 50L));
    }

    @Test
    @DisplayName("completeFuture for an unknown message id is a no-op")
    void completeFutureUnknownMessageIdIsNoOp() {
        assertDoesNotThrow(() ->
                handler.completeFuture("nobody-waiting", new UtilsRecords.HttpProxyResponse("x", false, 0, "", null)));
    }

    @Test
    @DisplayName("waitForResponse removes the registration so a stale completion is ignored afterwards")
    void waitForResponseCleansUpRegistration() {
        String messageId = "msg-cleanup";
        handler.register(messageId);
        assertNull(handler.waitForResponse(messageId, 50L)); // times out and removes the entry

        handler.completeFuture(messageId, new UtilsRecords.HttpProxyResponse(messageId, false, 0, "", null));
        assertNull(handler.waitForResponse(messageId, 50L));
    }

    @Test
    @DisplayName("waitForResponse returns null when the future completes exceptionally")
    void waitForResponseReturnsNullOnExecutionException() throws Exception {
        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        handler.register(messageId);
        getRegisteredFuture(messageId).completeExceptionally(new IllegalStateException("downstream failure"));

        assertNull(handler.waitForResponse(messageId, 2000L));
    }

    @Test
    @DisplayName("waitForResponse returns null and restores the interrupt flag when the waiting thread is interrupted")
    void waitForResponseReturnsNullOnInterruption() {
        String messageId = System.currentTimeMillis() + "-" + System.nanoTime();
        handler.register(messageId);

        Thread.currentThread().interrupt(); // future.get() throws InterruptedException immediately
        UtilsRecords.HttpProxyResponse result = handler.waitForResponse(messageId, 2000L);

        assertTrue(Thread.interrupted()); // the handler re-interrupted the thread; this also clears the flag
        assertNull(result);
    }

    @SuppressWarnings("unchecked")
    private CompletableFuture<UtilsRecords.HttpProxyResponse> getRegisteredFuture(String messageId) throws Exception {
        Field field = ProxyResponseHandler.class.getDeclaredField("proxyResponseMap");
        field.setAccessible(true);
        var map = (ConcurrentMap<String, CompletableFuture<UtilsRecords.HttpProxyResponse>>) field.get(handler);
        return map.get(messageId);
    }
}
