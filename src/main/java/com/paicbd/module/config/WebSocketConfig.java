package com.paicbd.module.config;

import com.paicbd.module.components.CustomFrameHandler;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.smsc.dto.UtilsRecords;
import com.paicbd.smsc.utils.Generated;
import com.paicbd.smsc.utils.WebsocketConstants;
import com.paicbd.smsc.ws.SocketClient;
import com.paicbd.smsc.ws.SocketSession;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Slf4j
@Generated
@Configuration
@RequiredArgsConstructor
public class WebSocketConfig {
    private final AppProperties appProperties;
    private final SocketSession socketSession;
    private final CustomFrameHandler customFrameHandler;

    @Bean
    public SocketClient socketClient() {
        List<String> topicsToSubscribe = List.of(
                WebsocketConstants.UPDATE_SMPP_SERVICE_PROVIDER,
                WebsocketConstants.DELETE_SMPP_SERVICE_PROVIDER,
                WebsocketConstants.UPDATE_GENERAL_SETTINGS_SMPP_HTTP,
                WebsocketConstants.UPDATE_SMPP_SERVER_LISTENER
        );

        UtilsRecords.WebSocketConnectionParams wsp = new UtilsRecords.WebSocketConnectionParams(
                appProperties.isWsEnabled(),
                appProperties.getWsHost(),
                appProperties.getWsPort(),
                appProperties.getWsPath(),
                topicsToSubscribe,
                appProperties.getWebsocketHeaderName(),
                appProperties.getWebsocketHeaderValue(),
                appProperties.getWebsocketRetryInterval(),
                "SMPP-SERVER" // Current SMSC Module
        );

        return new SocketClient(customFrameHandler, wsp, socketSession);
    }
}
