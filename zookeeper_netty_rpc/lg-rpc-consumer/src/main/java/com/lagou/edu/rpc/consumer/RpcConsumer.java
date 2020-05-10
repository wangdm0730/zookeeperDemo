package com.lagou.edu.rpc.consumer;

import com.lagou.edu.rpc.common.RpcRequest;
import com.lagou.edu.rpc.common.listener.NodeChangeListener;
import com.lagou.edu.rpc.common.metrics.RequestMetrics;
import com.lagou.edu.rpc.common.registry.RpcRegistryHandler;
import com.lagou.edu.rpc.consumer.client.RpcClient;
import com.lagou.edu.rpc.consumer.loadbalance.LoadBalanceStrategy;
import com.lagou.edu.rpc.consumer.loadbalance.impl.MinCostLoadBalance;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

public class RpcConsumer implements NodeChangeListener {

    private static final Map<String, List<RpcClient>> CLIENT_POOL = new ConcurrentHashMap<>();
    private RpcRegistryHandler rpcRegistryHandler;
    private Map<String, Object> serviceMap;
    private LoadBalanceStrategy loadBalance = new MinCostLoadBalance();

    /**
     * 初始化
     *
     * @param rpcRegistryHandler
     * @param serviceMap
     */
    public RpcConsumer(RpcRegistryHandler rpcRegistryHandler, Map<String, Object> serviceMap) {
        this.rpcRegistryHandler = rpcRegistryHandler;
        this.serviceMap = serviceMap;

        // 开始自动注册消费者逻辑
        serviceMap.entrySet().forEach(new Consumer<Map.Entry<String, Object>>() {
            @Override
            public void accept(Map.Entry<String, Object> entry) {
                String interfaceName = entry.getKey();
                List<String> discovery = rpcRegistryHandler.discovery(interfaceName);

                List<RpcClient> rpcClients = CLIENT_POOL.get(interfaceName);
                if (CollectionUtils.isEmpty(rpcClients)) {
                    rpcClients = new ArrayList<>();
                }
                for (String item : discovery) {
                    String[] split = item.split(":");
                    RpcClient rpcClient = new RpcClient(split[0], Integer.parseInt(split[1]));
                    try {
                        rpcClient.initClient(interfaceName);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    rpcClients.add(rpcClient);
                    CLIENT_POOL.put(interfaceName, rpcClients);
                }
            }
        });
        rpcRegistryHandler.addListener(this);
    }

    //1.创建一个代理对象
    public Object createProxy(final Class<?> serviceClass) {

        //借助JDK动态代理生成代理对象
        return Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(), new Class<?>[]{serviceClass}, (proxy, method, args) -> {

            //（1）调用初始化netty客户端的方法
            String serviceClassName = serviceClass.getName();

            // 封装request对象
            RpcRequest request = new RpcRequest();
            String requestId = UUID.randomUUID().toString();

            String className = method.getDeclaringClass().getName();
            String methodName = method.getName();

            Class<?>[] parameterTypes = method.getParameterTypes();

            request.setRequestId(requestId);
            request.setClassName(className);
            request.setMethodName(methodName);
            request.setParameterTypes(parameterTypes);
            request.setParameters(args);

            // 打印request
            System.out.println("请求内容: " + request);

            // 去服务端请求数据
            RpcClient rpcClient = loadBalance.route(CLIENT_POOL, serviceClassName);
            if (null == rpcClient) {
                return null;
            }
            try {
                return rpcClient.send(request);
            } catch (Exception e) {
                if (e.getClass().getName().equals("java.nio.channels.ClosedChannelException")) {
                    System.out.println("发送发生异常, 稍后重试:" + e.getMessage());
                    e.printStackTrace();
                    Thread.sleep(3000);
                    RpcClient otherRpcClient = loadBalance.route(CLIENT_POOL, serviceClassName);
                    if (null == otherRpcClient) {
                        return null;
                    }
                    return otherRpcClient.send(request);
                }
                throw e;
            }
        });
    }

    @Override
    public void notify(String service, List<String> serviceList, PathChildrenCacheEvent pathChildrenCacheEvent) {
        List<RpcClient> rpcClients = CLIENT_POOL.get(service);
        PathChildrenCacheEvent.Type eventType = pathChildrenCacheEvent.getType();
        System.out.println("收到节点变更通知:" + eventType + "----" + rpcClients + "---" + service + "---" + serviceList);
        String path = pathChildrenCacheEvent.getData().getPath();
        String instanceConfig = path.substring(path.lastIndexOf("/") + 1);

        // 增加节点
        if (PathChildrenCacheEvent.Type.CHILD_ADDED.equals(eventType)
                || PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED.equals(eventType)) {
            if (CollectionUtils.isEmpty(rpcClients)) {
                rpcClients = new ArrayList<>();
            }
            String[] address = instanceConfig.split(":");
            RpcClient client = new RpcClient(address[0], Integer.parseInt(address[1]));
            try {
                client.initClient(service);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            rpcClients.add(client);

            // 节点耗时统计
            RequestMetrics.getInstance().addNode(address[0], Integer.parseInt(address[1]), service);
            System.out.println("新增节点:" + instanceConfig);
        } else if (PathChildrenCacheEvent.Type.CHILD_REMOVED.equals(eventType)
                || PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED.equals(eventType)
                || PathChildrenCacheEvent.Type.CONNECTION_LOST.equals(eventType)) {
            // 移除节点
            if (CollectionUtils.isNotEmpty(rpcClients)) {
                String[] address = instanceConfig.split(":");
                for (int i = 0; i < rpcClients.size(); i++) {
                    RpcClient item = rpcClients.get(i);
                    if (item.getIp().equalsIgnoreCase(address[0]) && Integer.parseInt(address[1]) == item.getPort()) {

                        // 关闭连接
                        item.close();

                        // 从可用列表中移除
                        rpcClients.remove(item);
                        System.out.println("移除节点:" + instanceConfig);
                        RequestMetrics.getInstance().removeNode(address[0], Integer.parseInt(address[1]));
                    }
                }
            }
        }
    }
}
