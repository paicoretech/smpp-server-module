package com.paicbd.module;

import com.paicbd.smsc.utils.Generated;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@Generated
@EnableAsync
@EnableScheduling
@SpringBootApplication
public class SmppServerModuleApplication {
    public static void main(String[] args) {
        SpringApplication.run(SmppServerModuleApplication.class, args);
    }
}
