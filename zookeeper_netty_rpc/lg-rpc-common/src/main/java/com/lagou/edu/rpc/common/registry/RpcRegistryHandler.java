package com.lagou.edu.rpc.common.registry;

import com.lagou.edu.rpc.common.listener.NodeChangeListener;

import java.util.List;

/**
 * 注册中心，用来做服务注册、服务发现
 */
public interface RpcRegistryHandler {

    /**
     * 服务注册
     *
     * @param service
     * @param ip
     * @param port
     * @return
     */
    boolean registry(String service, String ip, int port);

    /**
     * 服务发现
     *
     * @param service
     * @return
     */
    List<String> discovery(String service);

    /**
     * 添加监听者
     *
     * @param listener
     */
    void addListener(NodeChangeListener listener);

    /**
     * 注册中心销毁
     */
    void destroy();
}
