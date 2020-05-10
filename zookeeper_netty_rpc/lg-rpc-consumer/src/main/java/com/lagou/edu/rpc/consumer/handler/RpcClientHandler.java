package com.lagou.edu.rpc.consumer.handler;

import com.lagou.edu.rpc.common.RpcResponse;
import com.lagou.edu.rpc.common.idle.Beat;
import com.lagou.edu.rpc.common.metrics.RequestMetrics;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * 处理服务端响应
 */
public class RpcClientHandler extends SimpleChannelInboundHandler<RpcResponse> {

    /**
     * 收到服务端数据
     */
    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, RpcResponse rpcResponse) throws Exception {
        RequestMetrics.getInstance().calculate(rpcResponse.getRequestId());
        System.out.println("请求id:" + rpcResponse.getRequestId() + ", 返回结果:" + rpcResponse.getResult());
    }

    /**
     * 定时处理
     *
     * @param ctx
     * @param evt
     * @throws Exception
     */
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt instanceof IdleStateEvent) {
            ctx.writeAndFlush(Beat.BEAT_PING);
        } else {
            super.userEventTriggered(ctx, evt);
        }
    }
}
