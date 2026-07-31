package com.paicbd.module.utils;

import com.paicbd.module.config.PendingDlrCache;
import com.paicbd.smsc.dto.ServiceProvider;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.session.Session;
import org.springframework.kafka.core.KafkaTemplate;
import com.paicbd.smsc.utils.RedisManager;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import static com.paicbd.module.utils.Constants.STOPPED;

@Slf4j(topic = "ServiceProviderSession")
public class SpSession {
    @Getter
    private final RedisManager redisManager;
    @Getter
    private final ServiceProvider currentServiceProvider;
    @Getter
    private final List<Session> currentSmppSessions = new ArrayList<>();
    @Getter
    private final PendingDlrCache pendingDlrCache;
    private final ThreadFactory factory = Thread.ofVirtual().name("Scheduled", 0).factory();
    private final ScheduledExecutorService deliveryExecService = Executors.newScheduledThreadPool(0, factory);

    private Boolean hasAvailableCredit;
    private int currentIndexRoundRobin = 0;

    public SpSession(RedisManager redisManager, ServiceProvider serviceProvider,
                     AppProperties appProperties, KafkaTemplate<String, String> kafkaTemplate) {
        this.redisManager = redisManager;
        this.currentServiceProvider = serviceProvider;
        this.pendingDlrCache = new PendingDlrCache(appProperties, kafkaTemplate);
        this.init();
    }

    public void init() {
        log.info("Initializing SpSession Class for service provider {}", currentServiceProvider.getSystemId());
        this.hasAvailableCredit = currentServiceProvider.getHasAvailableCredit();
    }

    public void updateRedis() {
        // Using this to skip backslash coming from regex in redis
        this.redisManager.hset(GeneralSmscConstants.SERVICE_PROVIDERS_HASH_NAME,
                String.valueOf(this.currentServiceProvider.getNetworkId()), this.currentServiceProvider.toString());
    }

    public Boolean hasAvailableCredit() {
        return this.hasAvailableCredit;
    }

    // This method returns true only if the service provider is stopped, is necessary return true to send notification to backend app via websocket
    public boolean updateCurrentServiceProvider(ServiceProvider newSp) {
        log.debug("Sp: {}", newSp.toString());
        log.debug("Updating current service provider, current Smpp Sessions: {}", currentSmppSessions.size());
        this.currentServiceProvider.setNetworkId(newSp.getNetworkId());
        this.currentServiceProvider.setName(newSp.getName());
        this.currentServiceProvider.setSystemId(newSp.getSystemId());
        this.currentServiceProvider.setPassword(newSp.getPassword());
        this.currentServiceProvider.setSystemType(newSp.getSystemType());
        this.currentServiceProvider.setAddressNpi(newSp.getAddressNpi());
        this.currentServiceProvider.setAddressRange(newSp.getAddressRange());
        this.currentServiceProvider.setInterfaceVersion(newSp.getInterfaceVersion());
        this.currentServiceProvider.setValidity(newSp.getValidity());
        this.currentServiceProvider.setTps(newSp.getTps());
        this.currentServiceProvider.setCredit(newSp.getCredit());
        this.currentServiceProvider.setIsPrepaid(newSp.getIsPrepaid());
        this.currentServiceProvider.setMaxBinds(newSp.getMaxBinds());
        this.currentServiceProvider.setStatus(newSp.getStatus());
        this.currentServiceProvider.setEnabled(newSp.getEnabled());
        this.currentServiceProvider.setAddressTon(newSp.getAddressTon());
        this.currentServiceProvider.setEnquireLinkPeriod(newSp.getEnquireLinkPeriod());
        this.currentServiceProvider.setPduTimeout(newSp.getPduTimeout());
        this.currentServiceProvider.setHasAvailableCredit(newSp.getHasAvailableCredit());
        this.currentServiceProvider.setBindType(newSp.getBindType());
        this.currentServiceProvider.setSmppServerId(newSp.getSmppServerId());
        this.currentServiceProvider.setMessagePriority(newSp.getMessagePriority());
        this.currentServiceProvider.setDlrTlvEnabled(newSp.isDlrTlvEnabled());
        this.currentServiceProvider.setProxyMode(newSp.isProxyMode());
        this.hasAvailableCredit = newSp.getHasAvailableCredit();

        if (newSp.getEnabled() == 0) { // enabled = 0 means that the service provider is stopped
            log.debug("Stopping service provider {}", this.currentServiceProvider.getSystemId());
            this.currentServiceProvider.setStatus(STOPPED);
            this.currentServiceProvider.getBinds().clear();

            List<Session> currentSmppSessionsToClose = new ArrayList<>(this.currentSmppSessions);
            currentSmppSessionsToClose.forEach(Session::unbindAndClose);

            this.currentServiceProvider.setCurrentBindsCount(0);
            this.currentSmppSessions.clear();
            this.updateRedis();
            return true;
        }

        this.updateRedis();
        return false;
    }

    public Session getNextRoundRobinSession() {
        if (currentSmppSessions.isEmpty()) {
            return null;
        }

        int originalIndex = currentIndexRoundRobin;
        int size = currentSmppSessions.size();

        if(currentIndexRoundRobin >= currentSmppSessions.size()) {
            currentIndexRoundRobin = 0;
        }

        for (int i = 0; i < size; i++) {
            Session session = currentSmppSessions.get(currentIndexRoundRobin);
            currentIndexRoundRobin = (currentIndexRoundRobin + 1) % size;
            if (session.getSessionState().isReceivable()) {
                return session;
            }
        }
        currentIndexRoundRobin = originalIndex;
        return null;
    }

    public void autoDestroy() {
        log.info("Auto destroy SpSession for systemId {}", this.currentServiceProvider.getSystemId());
        this.pendingDlrCache.publishPendingFailureCdrsAndClear();
        this.updateRedis();
        this.deliveryExecService.shutdown();
    }
}
