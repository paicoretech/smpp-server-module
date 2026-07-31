package com.paicbd.module.e2e;

import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jsmpp.extra.SessionState;
import org.jsmpp.session.Session;
import org.jsmpp.session.SessionStateListener;


@Slf4j
@NoArgsConstructor
public class SessionStateListenerImplMock implements SessionStateListener {

    @Override
    public synchronized void onStateChange(SessionState newState, SessionState oldState, Session source) {
        log.info("smpp client from old state {} to new state {} ", oldState, newState);
    }
}
