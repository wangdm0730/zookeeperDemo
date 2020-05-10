package com.lagou.edu.rpc.provider.server.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@AllArgsConstructor
@NoArgsConstructor
@Data
@Builder
public class RpcServerConfig {

    /**
     * 应用名
     */
    private String applicationName;

    /**
     * 端口号
     */
    private int port;

    /**
     * 本机IP
     */
    private String ip;

    /**
     * 延迟暴露
     */
    private int delay;

    /**
     * 是否为提供者
     */
    private boolean providerSide;

    /**
     * 扫描的服务
     */
    private Map<String, Object> services;

    /**
     * 服务列表
     */
    private List<String> serviceList;
}