package com.lagou.edu.rpc.provider.service;

import com.lagou.edu.rpc.api.UserService;
import com.lagou.edu.rpc.common.annotations.RpcService;
import org.springframework.stereotype.Service;

import java.util.Random;

@RpcService
@Service
public class UserServiceImpl implements UserService {

    @Override
    public String sayHello(String word) {
        Random random = new Random();
        int millis = random.nextInt(200);
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println("接到请求:" + word + ", 服务端随机sleep:" + millis + "ms");
        return word;
    }
}