package com.paicbd.module.components;

import com.paicbd.smsc.dto.GeneralSettings;
import com.paicbd.smsc.utils.GeneralSmscConstants;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import com.paicbd.smsc.utils.RedisManager;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class GeneralSettingsCacheConfigTest {
    @Mock
    private RedisManager redisManager;

    @InjectMocks
    private GeneralSettingsCacheConfig generalSettingsCacheConfig;

    @Test
    @DisplayName("initializeGeneralSettings initializing general settings")
    void initializeGeneralSettingsWhenInitGeneralSettingThenDoItSuccessfully() {
        GeneralSettings generalSettingsMock = GeneralSettings.builder()
                .id(1)
                .validityPeriod(60)
                .maxValidityPeriod(240)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(3)
                .encodingGsm7(0)
                .encodingUcs2(2)
                .build();

        when(this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http"))
                .thenReturn(generalSettingsMock.toString());

        generalSettingsCacheConfig.initializeGeneralSettings();

        GeneralSettings generalSettingsStored = this.generalSettingsCacheConfig.getCurrentGeneralSettings();
        assertEquals(generalSettingsMock.toString(), generalSettingsStored.toString());

        verify(this.redisManager).hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http");
    }

    @Test
    @DisplayName("initializeGeneralSettings initializing general settings when does not exists in Redis")
    void initializeGeneralSettingsWhenInitGeneralSettingAndDoesNotExistsThenThrowRuntimeException() {
        when(this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http")).thenReturn(null);

        assertThrows(RuntimeException.class, () -> generalSettingsCacheConfig.initializeGeneralSettings());
        verify(this.redisManager).hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http");
        assertNull(generalSettingsCacheConfig.getCurrentGeneralSettings());
    }

    @Test
    @DisplayName("initializeGeneralSettings updating general settings")
    void initializeGeneralSettingsWhenUpdateGeneralSettingsThenDoItSuccessfully() {
        GeneralSettings previousGeneralSettingsMock = GeneralSettings.builder()
                .id(1)
                .validityPeriod(60)
                .maxValidityPeriod(240)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(3)
                .encodingGsm7(0)
                .encodingUcs2(2)
                .build();

        GeneralSettings updatedGeneralSettingsMock = GeneralSettings.builder()
                .id(1)
                .validityPeriod(120)
                .maxValidityPeriod(240)
                .sourceAddrTon(5)
                .sourceAddrNpi(6)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(1)
                .encodingGsm7(1)
                .encodingUcs2(1)
                .build();

        // init general settings
        when(this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http"))
                .thenReturn(previousGeneralSettingsMock.toString());
        generalSettingsCacheConfig.initializeGeneralSettings();
        GeneralSettings previousGeneralSettingStored = this.generalSettingsCacheConfig.getCurrentGeneralSettings();

        // updating general settings
        when(this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http"))
                .thenReturn(updatedGeneralSettingsMock.toString());
        assertTrue(generalSettingsCacheConfig.updateGeneralSettings());
        GeneralSettings updatedGeneralSettingStored = this.generalSettingsCacheConfig.getCurrentGeneralSettings();

        // compare previous and current updated
        assertNotEquals(previousGeneralSettingStored.toString(), updatedGeneralSettingStored.toString());

        // verify assert for Redis
        verify(this.redisManager, times(2)).hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http");
    }

    @Test
    @DisplayName("updateGeneralSettings getting null from Redis")
    void updateGeneralSettingsWhenGettingNullFromRedisThenDoNothing() {
        GeneralSettings previousGeneralSettingsMock = GeneralSettings.builder()
                .id(1)
                .validityPeriod(60)
                .maxValidityPeriod(240)
                .sourceAddrTon(1)
                .sourceAddrNpi(1)
                .destAddrTon(1)
                .destAddrNpi(1)
                .encodingIso88591(3)
                .encodingGsm7(0)
                .encodingUcs2(2)
                .build();

        // init general settings
        when(this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http"))
                .thenReturn(previousGeneralSettingsMock.toString());
        generalSettingsCacheConfig.initializeGeneralSettings();

        // updating general settings
        when(this.redisManager.hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http"))
                .thenReturn(null);
        assertFalse(generalSettingsCacheConfig.updateGeneralSettings());

        // after updating general settings was not changed
        assertEquals(previousGeneralSettingsMock.toString(), this.generalSettingsCacheConfig.getCurrentGeneralSettings().toString());

        // verify assert for Redis
        verify(this.redisManager, times(2)).hget(GeneralSmscConstants.GENERAL_SETTINGS_HASH_NAME, "smpp_http");
    }
}
