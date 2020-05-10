package com.lagou.edu.rpc.consumer.loadbalance;

import com.lagou.edu.rpc.consumer.client.RpcClient;
import org.apache.commons.collections4.MapUtils;

import java.util.List;
import java.util.Map;

/**
 * 负载均衡策略抽象类
 */
public abstract class AbstractLoadBalance implements LoadBalanceStrategy {

    @Override
    public RpcClient route(Map<String, List<RpcClient>> clientPool, String serviceClassName) {
        if (MapUtils.isEmpty(clientPool)) {
            return null;
        }
        List<RpcClient> t = clientPool.get(serviceClassName);
        if (null == t) {
            return null;
        }
        return doSelect(t);
    }

    protected abstract RpcClient doSelect(List<RpcClient> t);
}
