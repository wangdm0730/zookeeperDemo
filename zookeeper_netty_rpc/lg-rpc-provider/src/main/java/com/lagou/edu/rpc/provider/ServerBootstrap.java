package com.lagou.edu.rpc.provider;

import com.lagou.edu.rpc.common.ConfigKeeper;
import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@ComponentScan(value = "com.lagou.edu")
@SpringBootApplication
public class ServerBootstrap {

    private static final String ZOOKEEPER_ADDRESS = "192.168.159.158:2181";
    private static final int DEFAULT_REPORT_INTERVAL = 5;

    public static void main(String[] args) throws Exception {
        int port = 8990;
        if (args.length > 0 && NumberUtils.isDigits(args[0])) {
            port = Integer.parseInt(args[0]);
        }

        ConfigKeeper configKeeper = ConfigKeeper.getInstance();
        configKeeper.setPort(port);
        configKeeper.setZkAddr(ZOOKEEPER_ADDRESS);
        configKeeper.setProviderSide(true);

        SpringApplication.run(ServerBootstrap.class, args);
    }
}