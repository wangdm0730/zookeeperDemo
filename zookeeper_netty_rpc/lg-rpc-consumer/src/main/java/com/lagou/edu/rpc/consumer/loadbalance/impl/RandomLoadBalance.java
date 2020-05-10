package com.lagou.edu.rpc.consumer.loadbalance.impl;

import com.lagou.edu.rpc.consumer.client.RpcClient;
import com.lagou.edu.rpc.consumer.loadbalance.AbstractLoadBalance;

import java.util.List;
import java.util.Random;

/**
 * 随机负载策略
 */
public class RandomLoadBalance extends AbstractLoadBalance {

    @Override
    protected RpcClient doSelect(List<RpcClient> t) {
        int length = t.size();
        Random random = new Random();
        return t.get(random.nextInt(length));
    }
}