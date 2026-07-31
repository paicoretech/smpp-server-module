package com.paicbd.module.components;

import com.paicbd.smsc.dto.UtilsRecords;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.ArgumentMatchers.any;

@ExtendWith(MockitoExtension.class)
class ProxyResponseListenerTest {

    @Mock
    ProxyResponseHandler proxyResponseHandler;

    @InjectMocks
    ProxyResponseListener proxyResponseListener;

    @Test
    @DisplayName("Each downstream response completes the matching future by message id")
    void listenCompletesFutureByMessageId() {
        UtilsRecords.HttpProxyResponse r1 = new UtilsRecords.HttpProxyResponse("m1", false, 0, "", null);
        UtilsRecords.HttpProxyResponse r2 = new UtilsRecords.HttpProxyResponse("m2", true, 8, "errorMessage", null);

        proxyResponseListener.listenProxyResponses(List.of(r1.toString(), r2.toString()));

        verify(proxyResponseHandler).completeFuture(eq("m1"), any(UtilsRecords.HttpProxyResponse.class));
        verify(proxyResponseHandler).completeFuture(eq("m2"), any(UtilsRecords.HttpProxyResponse.class));
    }

    @Test
    @DisplayName("Empty or null batches are ignored")
    void listenIgnoresEmptyBatch() {
        proxyResponseListener.listenProxyResponses(Collections.emptyList());
        proxyResponseListener.listenProxyResponses(null);
        verifyNoInteractions(proxyResponseHandler);
    }

    @Test
    @DisplayName("Responses without a message id are discarded")
    void listenDiscardsResponseWithoutMessageId() {
        UtilsRecords.HttpProxyResponse noId = new UtilsRecords.HttpProxyResponse(null, false, 0, "", null);
        UtilsRecords.HttpProxyResponse valid = new UtilsRecords.HttpProxyResponse("m3", false, 0, "", null);

        proxyResponseListener.listenProxyResponses(List.of(noId.toString(), valid.toString()));

        verify(proxyResponseHandler, times(1)).completeFuture(eq("m3"), any(UtilsRecords.HttpProxyResponse.class));
    }
}
