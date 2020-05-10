package com.lagou.edu.rpc.common.listener;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.List;

/**
 * 节点变更listener
 */
public interface NodeChangeListener {

    /**
     * 节点变更时通知listener
     *
     * @param children
     * @param serviceList
     * @param pathChildrenCacheEvent
     */
    void notify(String children, List<String> serviceList, PathChildrenCacheEvent pathChildrenCacheEvent);
}
