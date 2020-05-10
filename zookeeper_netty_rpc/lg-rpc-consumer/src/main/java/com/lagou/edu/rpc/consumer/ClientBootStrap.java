package com.lagou.edu.rpc.consumer;


import com.lagou.edu.rpc.api.UserService;
import com.lagou.edu.rpc.common.ConfigKeeper;
import com.lagou.edu.rpc.common.registry.RpcRegistryHandler;
import com.lagou.edu.rpc.registry.handler.impl.ZookeeperRegistryHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * 客户端启动类
 */
public class ClientBootStrap {

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> instanceCacheMap = new HashMap<>();
        instanceCacheMap.put(UserService.class.getName(), UserService.class);

        ConfigKeeper.getInstance().setConsumerSide(true);
        ConfigKeeper.getInstance().setInterval(5);

        RpcRegistryHandler rpcRegistryHandler = new ZookeeperRegistryHandler("192.168.159.158:2181");
        RpcConsumer consumer = new RpcConsumer(rpcRegistryHandler, instanceCacheMap);

        UserService userService = (UserService) consumer.createProxy(UserService.class);

        while (true) {
            Thread.sleep(2000);
            userService.sayHello("are you ok?");
        }
    }
}
