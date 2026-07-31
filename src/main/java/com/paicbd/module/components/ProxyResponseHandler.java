package com.paicbd.module.components;

import com.paicbd.smsc.dto.UtilsRecords;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@Component
public class ProxyResponseHandler {
    private final ConcurrentMap<String, CompletableFuture<UtilsRecords.HttpProxyResponse>> proxyResponseMap = new ConcurrentHashMap<>();

    public void register(String messageId) {
        proxyResponseMap.putIfAbsent(messageId, new CompletableFuture<>());
    }

    public UtilsRecords.HttpProxyResponse waitForResponse(String messageId, long timeoutMillis) {
        CompletableFuture<UtilsRecords.HttpProxyResponse> future = proxyResponseMap.get(messageId);
        if (Objects.isNull(future)) {
            log.warn("No proxy response future registered for message id {}", messageId);
            return null;
        }

        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            log.warn("Timeout waiting for proxy response for message id {} after {} ms", messageId, timeoutMillis);
            return null;
        } catch (ExecutionException e) {
            log.error("Error waiting for proxy response for message id {}", messageId, e);
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while waiting for proxy response for message id {}", messageId);
            return null;
        } finally {
            proxyResponseMap.remove(messageId);
        }
    }

    public void completeFuture(String messageId, UtilsRecords.HttpProxyResponse response) {
        CompletableFuture<UtilsRecords.HttpProxyResponse> future = proxyResponseMap.get(messageId);
        if (Objects.nonNull(future)) {
            future.complete(response);
        }
    }
}
