package com.lagou.edu.rpc.registry.handler;

import com.lagou.edu.rpc.common.ConfigKeeper;
import com.lagou.edu.rpc.common.registry.RpcRegistryHandler;
import com.lagou.edu.rpc.registry.handler.impl.ZookeeperRegistryHandler;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.stereotype.Service;

/**
 * 注册中心工厂类
 */
@Service
public class RpcRegistryFactory implements FactoryBean<RpcRegistryHandler>, DisposableBean {

    private RpcRegistryHandler rpcRegistryHandler;

    @Override
    public RpcRegistryHandler getObject() throws Exception {
        if (null != rpcRegistryHandler) {
            return rpcRegistryHandler;
        }
        rpcRegistryHandler = new ZookeeperRegistryHandler(ConfigKeeper.getInstance().getZkAddr());
        return rpcRegistryHandler;
    }

    @Override
    public Class<?> getObjectType() {
        return RpcRegistryHandler.class;
    }

    @Override
    public void destroy() throws Exception {
        if (null != rpcRegistryHandler) {
            rpcRegistryHandler.destroy();
        }
    }
}
