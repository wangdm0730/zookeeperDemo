package com.lagou.edu.rpc.consumer.loadbalance.impl;

import com.lagou.edu.rpc.common.metrics.RequestMetrics;
import com.lagou.edu.rpc.consumer.client.RpcClient;
import com.lagou.edu.rpc.consumer.loadbalance.AbstractLoadBalance;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * 最小耗时负载策略
 */
public class MinCostLoadBalance extends AbstractLoadBalance {

    @Override
    protected RpcClient doSelect(List<RpcClient> clients) {

        // 根据统计数据获取节点列表
        List<RequestMetrics.Metrics> allInstances = RequestMetrics.getInstance().getAllInstances();
        System.out.println(allInstances);
        if (CollectionUtils.isEmpty(allInstances)) {
            // 随机返回一个
            Random random = new Random();
            return clients.get(random.nextInt(clients.size()));
        }

        // 节点按照耗时排序
        Collections.sort(allInstances);

        // 如果只有一个节点，直接返回
        RequestMetrics.Metrics metrics0 = allInstances.get(0);
        if (allInstances.size() == 1) {
            return clients.stream().filter(rpcClient -> rpcClient.getIp().equals(metrics0.getIp()) &&
                    (rpcClient.getPort() == metrics0.getPort())).collect(Collectors.toList()).get(0);
        }

        // 多个节点的时候，取第二个跟第一个比较，如果耗时相同，说明有多个耗时相同的节点，需要把相同耗时的节点都取出来做随机
        RequestMetrics.Metrics metrics1 = allInstances.get(1);
        if (metrics0.getCost() == metrics1.getCost()) {
            List<RequestMetrics.Metrics> tempInstanceList = new ArrayList<>();
            allInstances.stream().forEach(metrics -> {
                if (metrics.getCost() == metrics0.getCost()) {
                    tempInstanceList.add(metrics);
                }
            });

            Random random = new Random();
            RequestMetrics.Metrics metrics = tempInstanceList.get(random.nextInt(tempInstanceList.size()));
            return clients.stream().filter(rpcClient -> rpcClient.getIp().equals(metrics.getIp()) &&
                    (rpcClient.getPort() == metrics.getPort())).collect(Collectors.toList()).get(0);
        }

        // 直接取第一个
        return clients.stream().filter(rpcClient -> rpcClient.getIp().equals(metrics0.getIp()) &&
                (rpcClient.getPort() == metrics0.getPort())).collect(Collectors.toList()).get(0);
    }
}