package com.paicbd.module.utils;

import com.paicbd.smsc.utils.Generated;

@Generated
public class Constants {
    private Constants() {
        throw new IllegalStateException("Utility class");
    }

    public static final String STARTED = "STARTED";
    public static final String STOPPED = "STOPPED";
    public static final String BINDING = "BINDING";
    public static final String BOUND = "BOUND";
    public static final String UNBINDING = "UNBINDING";
    public static final String TYPE = "sp";
    public static final String WEBSOCKET_STATUS_ENDPOINT = "/app/handler-status";
    public static final String RESPONSE_SMPP_SERVER_ENDPOINT = "/app/response-smpp-server";
    public static final String PARAM_UPDATE_STATUS = "status";
    public static final String PARAM_UPDATE_SESSIONS = "sessions";
    public static final String IEI_CONCATENATED_MESSAGE = "0x00";
    public static final String UPDATE_SMPP_SERVER_STATUS_NOTIFICATION = "/app/smpp-server/status/update";

    public static final String ACTION_STATUS = "success";
    public static final String STOPPED_STATUS = "STOPPED";
    public static final String STARTED_STATUS = "STARTED";
    public static final String FAILED_STATUS = "FAILED";
    public static final String FORCED_STOPPED_STATUS = "FORCE_STOPPED";
    public static final int STOPPED_SERVER = 0;
    public static final int STARTED_SERVER = 1;
    public static final int DELETED_SERVER = 2;

    public static final String ALL_BIND_TYPES = "ALL_BIND_TYPES";
}
