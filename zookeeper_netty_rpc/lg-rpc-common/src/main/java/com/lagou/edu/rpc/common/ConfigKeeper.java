package com.lagou.edu.rpc.common;

/**
 * 持有全局配置
 */
public class ConfigKeeper {

    private static volatile ConfigKeeper configKeeper;
    // 端口号
    private int port;
    // zookeeper地址
    private String zkAddr;
    // 主动上报间隔时间，单位:秒
    private int interval;
    // 客户端侧
    private boolean consumerSide;
    // 服务端侧
    private boolean providerSide;


    private ConfigKeeper() {

    }

    /**
     * 全局单例
     *
     * @return
     */
    public static ConfigKeeper getInstance() {
        if (null == configKeeper) {
            synchronized (ConfigKeeper.class) {
                configKeeper = new ConfigKeeper();
            }
        }
        return configKeeper;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getZkAddr() {
        return zkAddr;
    }

    public void setZkAddr(String zkAddr) {
        this.zkAddr = zkAddr;
    }

    public int getInterval() {
        return interval;
    }

    public void setInterval(int interval) {
        this.interval = interval;
    }

    public boolean isConsumerSide() {
        return consumerSide;
    }

    public void setConsumerSide(boolean consumerSide) {
        this.consumerSide = consumerSide;
        this.providerSide = !consumerSide;
    }

    public boolean isProviderSide() {
        return providerSide;
    }

    public void setProviderSide(boolean providerSide) {
        this.providerSide = providerSide;
        this.consumerSide = !providerSide;
    }
}
