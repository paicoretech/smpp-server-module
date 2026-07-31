package com.paicbd.module.components;

import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.exception.RTException;
import com.paicbd.smsc.utils.Converter;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import com.paicbd.smsc.utils.RedisManager;

import java.util.Objects;

@Slf4j
@Component
@RequiredArgsConstructor
public class GeneralSettingsCacheConfig {
    private final RedisManager redisManager;
    private GeneralSettings generalSettingsCache;

    @PostConstruct
    public void initializeGeneralSettings() {
        this.generalSettingsCache = getGeneralSettingFromRedis();
        if (Objects.isNull(this.generalSettingsCache)) {
            throw new RTException("GeneralSettings can not be null");
        }
    }

    public GeneralSettings getCurrentGeneralSettings() {
        return this.generalSettingsCache;
    }

    public boolean updateGeneralSettings() {
        var generalSettingsUpdated = getGeneralSettingFromRedis();
        if (Objects.isNull(generalSettingsUpdated)) {
            log.error("generalSettingsUpdated is null");
            return false;
        }
        this.generalSettingsCache = generalSettingsUpdated;
        return true;
    }

    public GeneralSettings getGeneralSettingFromRedis() {
        String value = this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, GeneralSmscConstants.GENERAL_SETTINGS_SMPP_HTTP_CONFIG_KEY);
        if (Objects.isNull(value)) {
            log.info("General settings was not found in Redis");
            return null;
        }
        return Converter.stringToObject(value, GeneralSettings.class);
    }
}
