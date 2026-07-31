package com.paicbd.module.server;

import com.paicbd.module.components.GeneralSettingsCacheConfig;
import com.paicbd.module.components.ProxyResponseHandler;
import com.paicbd.module.utils.AppProperties;
import com.paicbd.module.utils.SpSession;
import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.dto.SmppServerConfig;
import com.paicbd.smsc.scylla.ScyllaManager;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import com.paicbd.smsc.utils.RateLimiterUtilManager;
import com.paicbd.smsc.utils.UtilsEnum;
import com.paicbd.smsc.utils.Watcher;
import com.paicbd.smsc.ws.SocketSession;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.PDUStringException;
import org.jsmpp.SMPPConstant;
import org.jsmpp.bean.InterfaceVersion;
import org.jsmpp.session.BindRequest;
import org.jsmpp.session.SMPPServerSession;
import org.jsmpp.session.SMPPServerSessionListener;
import org.jsmpp.session.connection.ServerConnectionFactory;
import org.jsmpp.session.connection.socket.ServerSocketConnectionFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.simp.stomp.StompSession;
import com.paicbd.smsc.utils.RedisManager;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.paicbd.module.utils.Constants.ACTION_STATUS;
import static com.paicbd.module.utils.Constants.ALL_BIND_TYPES;
import static com.paicbd.module.utils.Constants.FAILED_STATUS;
import static com.paicbd.module.utils.Constants.FORCED_STOPPED_STATUS;
import static com.paicbd.module.utils.Constants.STARTED_SERVER;
import static com.paicbd.module.utils.Constants.STARTED_STATUS;
import static com.paicbd.module.utils.Constants.STOPPED;
import static com.paicbd.module.utils.Constants.STOPPED_SERVER;
import static com.paicbd.module.utils.Constants.STOPPED_STATUS;
import static com.paicbd.module.utils.Constants.UPDATE_SMPP_SERVER_STATUS_NOTIFICATION;
import static com.paicbd.smsc.utils.UtilsEnum.getInterfaceVersion;

@Slf4j
@RequiredArgsConstructor
public class CustomSmppServer implements Runnable {
    private static final AtomicInteger requestCounter = new AtomicInteger();
    private static final String ANY_INTERFACE_VERSION_NAME = "IF_ANY";

    private final ThreadFactory factory = Thread.ofVirtual().name("SMPP-SERVER-", 0).factory();
    private final ExecutorService execService = Executors.newThreadPerTaskExecutor(factory);
    private final AtomicBoolean isInstanceRunning = new AtomicBoolean(true);

    private final RedisManager redisManager;
    private final SocketSession socketSession;
    private final Set<ServiceProvider> providers;
    @Getter
    private final SmppServerConfig smppServerConfig;
    private final ConcurrentMap<Integer, SpSession> spSessionMap;
    private final GeneralSettingsCacheConfig generalSettingsCacheConfig;
    private final ScyllaManager scyllaManager;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RateLimiterUtilManager rateLimiterUtilManager;
    private final AppProperties appProperties;
    private final ProxyResponseHandler proxyResponseHandler;

    private SMPPServerSessionListener sessionListener;
    private MultiPartsHandler multiPartsHandler;

    @Override
    public void run() {
        if (multiPartsHandler == null) {
            multiPartsHandler = new MultiPartsHandler(scyllaManager, kafkaTemplate, appProperties.getMessagePartsTtl());
        }
        if (isPortAvailable(smppServerConfig.getPort())) {
            Thread.startVirtualThread(() -> new Watcher("SmppServer:InboundSubmitSm" + smppServerConfig.getName(), requestCounter, 1));
            smppServerConfig.setStatus(STARTED_STATUS);
            openPortForListen();
        } else {
            isInstanceRunning.set(false);
            String cause = String.format("The port %s is already in use", smppServerConfig.getPort());
            log.error("Error while starting server: {}", cause);
            sendNotification(FAILED_STATUS, cause);
        }
    }

    public void updateSmppServerConfig(SmppServerConfig newConfig) {
        smppServerConfig.setName(newConfig.getName());
        smppServerConfig.setIp(newConfig.getIp());
        smppServerConfig.setPort(newConfig.getPort());
        smppServerConfig.setProcessorDegree(newConfig.getProcessorDegree());
        smppServerConfig.setQueueCapacity(newConfig.getQueueCapacity());
        smppServerConfig.setTransactionTimer(newConfig.getTransactionTimer());
        smppServerConfig.setWaitForBind(newConfig.getWaitForBind());
        smppServerConfig.setEnabled(newConfig.getEnabled());
        smppServerConfig.setTlsEnabled(newConfig.isTlsEnabled());
    }

    private void openPortForListen() {
        ServerConnectionFactory connectionFactory;
        try {
            connectionFactory = smppServerConfig.isTlsEnabled()
                    ? new KeyStoreSSLServerConnectionFactory(
                    appProperties.getTlsKeystorePath(),
                    appProperties.getTlsKeystorePassword())
                    : new ServerSocketConnectionFactory();
        } catch (Exception e) {
            isInstanceRunning.set(false);
            log.error("Failed to create connection factory for port {}: {}", smppServerConfig.getPort(), e.getMessage());
            sendNotification(FAILED_STATUS, "TLS configuration error: " + e.getMessage());
            stop();
            return;
        }

        try (SMPPServerSessionListener currentSessionListener =
                     new SMPPServerSessionListener(InetAddress.getByName(smppServerConfig.getIp()), smppServerConfig.getPort(), connectionFactory)) {
            log.info("SMPP Server is listening {}:{} (TLS: {})", smppServerConfig.getIp(), smppServerConfig.getPort(), smppServerConfig.isTlsEnabled());
            sessionListener = currentSessionListener;
            sessionListener.setPduProcessorDegree(smppServerConfig.getProcessorDegree());
            sessionListener.setQueueCapacity(smppServerConfig.getQueueCapacity());
            prepareForAcceptConnections();
        } catch (IOException e) {
            isInstanceRunning.set(false);
            log.error("Error while opening port {} for listening", smppServerConfig.getPort(), e);
            sendNotification(FAILED_STATUS, e.getMessage());
            stop();
        }
    }

    private void handleConnection() throws IOException {
        SMPPServerSession session = sessionListener.accept();
        session.setTransactionTimer(smppServerConfig.getTransactionTimer());
        log.debug("Session accepted from {}", session.getSessionId());

        final SMPPServerSession finalSession = session;

        execService.submit(() -> {
            try {
                log.info("Waiting for bind {} milliseconds, session {}",
                        smppServerConfig.getWaitForBind(), finalSession.getSessionId());

                BindRequest bindRequest = finalSession.waitForBind(smppServerConfig.getWaitForBind());

                String systemId = bindRequest.getSystemId();

                Optional<ServiceProvider> provider = providers.stream()
                        .filter(p -> Objects.equals(p.getSystemId(), systemId))
                        .findFirst();

                if (provider.isEmpty()) {
                    log.warn("BIND FAIL:: SystemId {} not found", systemId);
                    bindRequest.reject(SMPPConstant.STAT_ESME_RINVSYSID);
                    finalSession.unbindAndClose();
                    return;
                }

                ServiceProvider currentProvider = provider.get();
                if (!passAllValidations(bindRequest, currentProvider, finalSession)) {
                    return;
                }

                log.info("Bind request accepted for systemId {}", systemId);


                SpSession currentSpSession = spSessionMap.putIfAbsent(currentProvider.getNetworkId(), new SpSession(redisManager, currentProvider, appProperties, kafkaTemplate));
                Objects.requireNonNull(currentSpSession, "An error occurred while creating SpSession in prepareForAcceptConnections.The value is incompatible with the expected class");

                log.info("Accepting bind for session {}, interface version {}", finalSession.getSessionId(), bindRequest.getInterfaceVersion());
                finalSession.setMessageReceiverListener(createMessageReceiverListener(currentSpSession));
                finalSession.addSessionStateListener(createSessionStateListener(currentProvider.getNetworkId()));
                bindRequest.accept(currentProvider.getSystemId(), bindRequest.getInterfaceVersion());
            } catch (TimeoutException e) {
                log.error("Timeout while waiting for bind from remote {}:{}", finalSession.getInetAddress().getHostAddress(), finalSession.getPort());
                finalSession.close();
            } catch (IOException e) {
                log.error("BIND FAIL:: IO exception while accepting session", e);
                finalSession.close();
            } catch (PDUStringException e) {
                log.error("BIND FAIL:: PDU string exception while accepting session", e);
                finalSession.close();
            } catch (Exception e) {
                log.error("Unexpected error in connection handler", e);
                finalSession.close();
            }
        });
    }

    private void prepareForAcceptConnections() {
        try {
            redisManager.hset(GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME, String.valueOf(smppServerConfig.getId()), smppServerConfig.toString());
            sendNotification(STARTED_STATUS, ACTION_STATUS);
            while (isInstanceRunning.get()) {
                handleConnection();
            }
        } catch (Exception e) {
            log.error("Fatal error in prepareForAcceptConnections", e);
        }
    }

    private boolean isInterfaceVersionMismatch(String providerInterfaceVersion, InterfaceVersion interfaceVersion) {
        if (providerInterfaceVersion == null || ANY_INTERFACE_VERSION_NAME.equalsIgnoreCase(providerInterfaceVersion)) {
            return false;
        }
        return !Objects.equals(getInterfaceVersion(providerInterfaceVersion), interfaceVersion);
    }

    private boolean passAllValidations(BindRequest bindRequest, ServiceProvider currentProvider, SMPPServerSession finalSession) {
        try {
            String password = bindRequest.getPassword();
            InterfaceVersion interfaceVersion = bindRequest.getInterfaceVersion();
            String systemType = bindRequest.getSystemType();
            String addressRange = Optional.ofNullable(bindRequest.getAddressRange()).orElse("");

            if (!Objects.equals(currentProvider.getEnabled(), 1)) {
                log.warn("BIND FAIL:: SystemId {} failed validation (not enabled)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RSYSERR);
                finalSession.unbindAndClose();
                return false;
            }

            if (!currentProvider.getPassword().equals(password)) {
                log.warn("BIND FAIL:: SystemId {} failed validation (password mismatch)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RINVPASWD);
                finalSession.unbindAndClose();
                return false;
            }

            String providerInterfaceVersion = currentProvider.getInterfaceVersion();
            if (isInterfaceVersionMismatch(providerInterfaceVersion, interfaceVersion)) {
                log.warn("BIND FAIL:: SystemId {} failed validation (interface version mismatch)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RBINDFAIL);
                finalSession.unbindAndClose();
                return false;
            }

            if (applyForValidation(currentProvider.getSystemType()) &&
                    !Objects.equals(currentProvider.getSystemType(), systemType)) {
                log.warn("BIND FAIL:: SystemId {} failed validation (system type mismatch)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RINVSYSTYP);
                finalSession.unbindAndClose();
                return false;
            }

            if (applyForValidation(currentProvider.getAddressRange()) &&
                    !addressRange.matches(currentProvider.getAddressRange())) {
                log.warn("BIND FAIL:: SystemId {} failed validation (address range mismatch)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RBINDFAIL);
                finalSession.unbindAndClose();
                return false;
            }

            if (!Objects.equals(currentProvider.getSmppServerId(), smppServerConfig.getId())) {
                log.warn("BIND FAIL:: SystemId {} failed validation (not allowed on this server)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RBINDFAIL);
                finalSession.unbindAndClose();
                return false;
            }

            if (STOPPED.equalsIgnoreCase(currentProvider.getStatus())) {
                log.info("BIND FAIL:: Provider {} is currently stopped", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RSYSERR);
                finalSession.unbindAndClose();
                return false;
            }

            boolean invalidBindType = !ALL_BIND_TYPES.equals(currentProvider.getBindType()) && !UtilsEnum.getBindType(currentProvider.getBindType()).equals(bindRequest.getBindType());
            if (invalidBindType) {
                log.info("BIND FAIL:: Provider {} failed validation (bind type mismatch)", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RBINDFAIL);
                finalSession.unbindAndClose();
                return false;
            }

            if (currentProvider.getCurrentBindsCount() >= currentProvider.getMaxBinds()) {
                log.info("BIND FAIL:: Provider {} has reached the maximum binds", currentProvider.getSystemId());
                bindRequest.reject(SMPPConstant.STAT_ESME_RBINDFAIL);
                finalSession.unbindAndClose();
                return false;
            }

            return true;
        } catch (IOException e) {
            log.error("Error while applying provider validations", e);
            return false;
        }
    }

    private void stopServer(String status, int enabled) {
        try {
            isInstanceRunning.set(false);
            if (Objects.nonNull(sessionListener)) {
                sessionListener.close();
            }
            smppServerConfig.setStatus(status);
            smppServerConfig.setEnabled(enabled);
            redisManager.hset(
                    GeneralSmscConstants.SMPP_SERVER_CONFIGURATIONS_HASH_NAME,
                    String.valueOf(smppServerConfig.getId()),
                    smppServerConfig.toString()
            );
            sendNotification(status, ACTION_STATUS);
        } catch (IOException e) {
            smppServerConfig.setStatus(STOPPED_STATUS);
            smppServerConfig.setEnabled(STOPPED_SERVER);
            log.error("Error while stopping server", e);
        }
    }

    public void stop() {
        stopServer(STOPPED_STATUS, STOPPED_SERVER);
    }

    public void forceStopped() {
        stopServer(FORCED_STOPPED_STATUS, STARTED_SERVER);
    }

    private ServerMessageReceiverListenerImpl createMessageReceiverListener(SpSession spSession) {
        return new ServerMessageReceiverListenerImpl(
                requestCounter,
                spSession,
                generalSettingsCacheConfig,
                multiPartsHandler,
                kafkaTemplate,
                rateLimiterUtilManager,
                proxyResponseHandler,
                appProperties.getProxyModeResponseTimeout()
        );
    }

    private SessionStateListenerImpl createSessionStateListener(int serviceProviderNetworkId) {
        StompSession stompSession = socketSession.getStompSession();
        GeneralSettings generalSettings = generalSettingsCacheConfig.getCurrentGeneralSettings();
        return new SessionStateListenerImpl(
                serviceProviderNetworkId,
                spSessionMap,
                stompSession,
                redisManager,
                generalSettings,
                kafkaTemplate,
                appProperties.isBindNotificationEnabled()
        );
    }

    private void sendNotification(String status, String message) {
        String finalMessage = String.format("%s|%s|%s", smppServerConfig.getId(), status, message);
        log.info("Sending notification to the client: {}", finalMessage);
        StompSession stompSession = socketSession.getStompSession();
        if (Objects.nonNull(stompSession)) {
            synchronized (stompSession) { // Prevent IllegalStateException: The remote endpoint was in state [TEXT_PARTIAL_WRITING] which is an invalid state for called method
                stompSession.send(UPDATE_SMPP_SERVER_STATUS_NOTIFICATION, finalMessage);
            }
        }
    }

    private boolean applyForValidation(String parameter) {
        return Objects.nonNull(parameter) && !parameter.isEmpty();
    }

    public static boolean isPortAvailable(int port) {
        try (ServerSocket serverSocket = new ServerSocket(port)) {
            serverSocket.setReuseAddress(true);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
