package com.lagou.edu.rpc.consumer.client;

import com.lagou.edu.rpc.common.RpcRequest;
import com.lagou.edu.rpc.common.RpcResponse;
import com.lagou.edu.rpc.common.codec.RpcDecoder;
import com.lagou.edu.rpc.common.codec.RpcEncoder;
import com.lagou.edu.rpc.common.idle.Beat;
import com.lagou.edu.rpc.common.metrics.RequestMetrics;
import com.lagou.edu.rpc.common.serialize.impl.JSONSerializer;
import com.lagou.edu.rpc.consumer.handler.RpcClientHandler;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 请求客户端
 */
public class RpcClient {

    private Channel channel;
    private EventLoopGroup group;
    private RpcClientHandler rpcClientHandler = new RpcClientHandler();
    private String ip;
    private int port;

    public RpcClient(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }


    //2.初始化netty客户端
    public void initClient(String serviceClassName) throws InterruptedException {
        this.group = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();

        bootstrap.group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 3000)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new IdleStateHandler(0, 0, Beat.BEAT_INTERVAL, TimeUnit.SECONDS));   // beat N, close if fail
                        pipeline.addLast(new RpcEncoder(RpcRequest.class, new JSONSerializer()));
                        pipeline.addLast(new RpcDecoder(RpcResponse.class, new JSONSerializer()));
                        pipeline.addLast(rpcClientHandler);
                    }
                });
        this.channel = bootstrap.connect(ip, port).sync().channel();

        // 判断连接是否有效
        if (!isValidate()) {
            close();
            return;
        }
        System.out.println("====启动客户端：" + serviceClassName + ", ip:" + ip + ", port:" + port + "=====");
    }

    /**
     * 判断连接是否有效
     *
     * @return
     */
    public boolean isValidate() {
        if (this.channel != null) {
            return this.channel.isActive();
        }
        return false;
    }

    /**
     * 关闭
     */
    public void close() {
        if (this.channel != null && this.channel.isActive()) {
            this.channel.close();
        }
        if (this.group != null && !this.group.isShutdown()) {
            this.group.shutdownGracefully();
        }
        System.out.println("rpc client close");
    }

    /**
     * 发送请求
     *
     * @param rpcRequest
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    public Object send(RpcRequest rpcRequest) throws InterruptedException, ExecutionException {
        // 开始统计请求时间
        RequestMetrics.getInstance().put(ip, port, rpcRequest.getRequestId(), rpcRequest.getClassName());
        return this.channel.writeAndFlush(rpcRequest).sync().get();
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RpcClient rpcClient = (RpcClient) o;
        return port == rpcClient.port &&
                ip.equals(rpcClient.ip);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip, port);
    }
}