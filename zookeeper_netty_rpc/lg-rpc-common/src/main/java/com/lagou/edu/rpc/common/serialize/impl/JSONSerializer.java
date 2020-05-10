package com.lagou.edu.rpc.common.serialize.impl;

import com.alibaba.fastjson.JSON;
import com.lagou.edu.rpc.common.serialize.Serializer;

/**
 * FastJson实现
 */
public class JSONSerializer implements Serializer {

    @Override
    public byte[] serialize(Object object) {
        return JSON.toJSONBytes(object);
    }

    @Override
    public <T> T deserialize(Class<T> clazz, byte[] bytes) {
        return JSON.parseObject(bytes, clazz);
    }
}
