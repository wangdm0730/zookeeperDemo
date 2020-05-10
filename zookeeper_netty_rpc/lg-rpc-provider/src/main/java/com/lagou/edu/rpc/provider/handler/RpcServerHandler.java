package com.lagou.edu.rpc.provider.handler;

import com.lagou.edu.rpc.common.RpcRequest;
import com.lagou.edu.rpc.common.RpcResponse;
import com.lagou.edu.rpc.common.annotations.RpcService;
import com.lagou.edu.rpc.common.idle.Beat;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;
import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.BeansException;
import org.springframework.cglib.reflect.FastClass;
import org.springframework.cglib.reflect.FastMethod;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 托管所有暴露接口的实现
 */
@Component
public class RpcServerHandler extends SimpleChannelInboundHandler<RpcRequest> implements ApplicationContextAware {

    private static final Map<String, Object> SERVICE_INSTANCE_MAP = new ConcurrentHashMap<>();

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        Map<String, Object> serviceBeanMap = applicationContext.getBeansWithAnnotation(RpcService.class);
        if (MapUtils.isNotEmpty(serviceBeanMap)) {
            Set<Map.Entry<String, Object>> entries = serviceBeanMap.entrySet();
            for (Map.Entry<String, Object> item : entries) {
                Object serviceBean = item.getValue();
                if (serviceBean.getClass().getInterfaces().length == 0) {
                    throw new RuntimeException("service must implements interface.");
                }
                String interfaceName = serviceBean.getClass().getInterfaces()[0].getName();
                SERVICE_INSTANCE_MAP.put(interfaceName, serviceBean);
            }
        }
    }

    /**
     * 处理客户端业务请求
     *
     * @param ctx
     * @param rpcRequest
     * @throws Exception
     */
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RpcRequest rpcRequest) throws Exception {
        if (Beat.BEAT_ID.equalsIgnoreCase(rpcRequest.getRequestId())) {
            System.out.println("===idle===");
            return;
        }
        RpcResponse response = new RpcResponse();
        response.setRequestId(rpcRequest.getRequestId());
        try {
            response.setResult(handler(rpcRequest));
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        ctx.writeAndFlush(response);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        System.out.println("发生异常:" + cause.getMessage());
        ctx.channel().close();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.channel().close();
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }

    /**
     * 业务逻辑处理
     *
     * @param request
     * @return
     * @throws InvocationTargetException
     */
    private Object handler(RpcRequest request) throws InvocationTargetException {

        Object serviceBean = SERVICE_INSTANCE_MAP.get(request.getClassName());

        Class<?> serviceClass = serviceBean.getClass();

        String methodName = request.getMethodName();

        Class<?>[] parameterTypes = request.getParameterTypes();
        Object[] parameters = request.getParameters();

        //使用CGLB Reflect
        FastClass fastClass = FastClass.create(serviceClass);
        FastMethod fastMethod = fastClass.getMethod(methodName, parameterTypes);

        return fastMethod.invoke(serviceBean, parameters);
    }
}
